<?php

declare(strict_types=1);

namespace Morbo\React\Mqtt;


use Exception;
use Morbo\React\Mqtt\Packets;
use Morbo\React\Mqtt\Protocols\VersionInterface;
use Morbo\React\Mqtt\Protocols\VersionViolation;
use Morbo\React\Mqtt\Utils;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Socket\TcpConnector;

use function React\Promise\reject;
use function React\Promise\resolve;

class Client
{
    /**
     * @var \React\Socket\TcpConnector
     */
    protected $connector;

    /**
     * @var \React\EventLoop\LoopInterface
     */
    protected $loop;

    /**
     * @var \Morbo\React\Mqtt\Protocols\VersionInterface
     */
    protected $version;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    protected $logger;

    /**
     * @var \React\EventLoop\TimerInterface
     */
    protected $keepAliveTimer;

    /**
     * @var string
     */
    protected $state;

    const STATE_INITIATED = 'initiated';
    const STATE_CONNECTING = 'connecting';
    const STATE_CONNECTED = 'connected';
    const STATE_DISCONNECTED = 'disconnected';

    public function __construct(
        LoopInterface $loop,
        VersionInterface $version,
        LoggerInterface $logger = null
    ) {
        $this->loop = $loop;
        $this->version = $version;
        $this->logger = $logger;
        $this->connector = new TcpConnector($loop);
        $this->messageCounter = 1;
        $this->state = self::STATE_INITIATED;

        if (!$this->logger) {
            $this->logger = new NullLogger();
        }
    }

    public function connect(string $host, ConnectionOptions $options = null)
    {
        $this->logger->debug(sprintf('Initiate connection to %s', $host));
        $this->state = self::STATE_CONNECTING;

        // Set default connection options, if none provided
        if ($options == null) {
            $options = $this->getDefaultConnectionOptions();
        }

        $promise = $this->connector->connect($host);

        $promise->then(function (ConnectionInterface $stream) {
            $this->listenPackets($stream);
        });

        $connection = $promise
            ->then(function (ConnectionInterface $stream) use ($options) {
                return $this->sendConnectPacket($stream, $options);
            })
            ->then(function (ConnectionInterface $stream) use ($options) {
                $this->state = self::STATE_CONNECTED;

                return $this->setupKeepAlive($stream, $options->keepAlive);
            })
            ->then(null, function (Exception $e) {
                if ($e instanceof ConnectionException) {
                    $this->logger->critical('Connection error', [$e->getMessage()]);
                }

                throw $e;
            });

        return $connection;
    }

    protected function listenPackets(ConnectionInterface $stream)
    {
        $stream->on('data', function ($raw) use ($stream) {
            try {
                foreach (Utils\PacketFactory::getNextPacket($this->version, $raw) as $packet) {
                    $this->logger->debug('Received packet: ' . get_class($packet));
                    $stream->emit($packet::EVENT, [$packet]);
                }
            } catch (VersionViolation $e) {
                $stream->emit('INVALID', [$e]);
            }
        });

        $stream->on('close', function () {
            $this->state = self::STATE_DISCONNECTED;
            $this->logger->debug('Stream was closed');
        });

        $this->logger->debug('Packets listened initiated');
    }

    protected function sendConnectPacket(
        ConnectionInterface $stream,
        ConnectionOptions $options
    ): PromiseInterface {
        $packet = new Packets\Connect(
            $this->version,
            $options->username,
            $options->password,
            $options->clientId,
            $options->cleanSession,
            $options->will,
            $options->keepAlive
        );

        $deferred = new Deferred();
        $stream->on(Packets\ConnectionAck::EVENT, function (Packets\ConnectionAck $ack) use ($stream, $deferred) {
            $this->logger->debug('Received ' . Packets\ConnectionAck::EVENT . ' event', ['statusCode' => $ack->getStatusCode()]);
            if ($ack->getConnected()) {
                $deferred->resolve($stream);
            }
            $deferred->reject(
                new ConnectionException('Unable to establish connection, statusCode is ' . $ack->getStatusCode())
            );
        });

        $this->sendPacketToStream($stream, $packet);

        return $deferred->promise();
    }

    protected function setupKeepAlive(ConnectionInterface $stream, int $interval)
    {
        if ($interval > 0) {
            $this->logger->debug('KeepAlive interval is ' . $interval);
            $this->loop->addPeriodicTimer($interval, function (TimerInterface $timer) use ($stream) {
                if ($this->state === self::STATE_CONNECTED) {
                    $packet = new Packets\PingRequest($this->version);
                    $this->sendPacketToStream($stream, $packet);
                }
                $this->keepAliveTimer = $timer;
            });
        }

        return resolve($stream);
    }

    public function subscribe(ConnectionInterface $stream, $topic, $qos = 0): PromiseInterface
    {
        if ($this->state !== self::STATE_CONNECTED) {
            return reject('Connection unavailable');
        }

        $subscribePacket = new Packets\Subscribe($this->version);
        $subscribePacket->addSubscription($topic, $qos);
        $this->sendPacketToStream($stream, $subscribePacket);
        $this->logger->debug('Send subscription, packetId: ' . $subscribePacket->getPacketId());

        $deferred = new Deferred();
        $stream->on(Packets\SubscribeAck::EVENT, function (Packets\SubscribeAck $ackPacket) use ($stream, $deferred, $subscribePacket) {
            if ($subscribePacket->getPacketId() === $ackPacket->getPacketId()) {
                $this->logger->debug('Subscription successful', [
                    'topic' => $subscribePacket->getTopic(),
                    'qos' => $subscribePacket->getQoS()
                ]);
                $deferred->resolve($stream);
            } else {
                $deferred->reject('Subscription ack has wrong packetId');
            }
        });

        return $deferred->promise();
    }

    public function unsubscribe(ConnectionInterface $stream, $topic): PromiseInterface
    {
        if ($this->state !== self::STATE_CONNECTED) {
            return reject('Connection unavailable');
        }

        $unsubscribePacket = new Packets\Unsubscribe($this->version);
        $unsubscribePacket->removeSubscription($topic);
        $this->sendPacketToStream($stream, $unsubscribePacket);

        $deferred = new Deferred();

        $stream->on(Packets\UnsubscribeAck::EVENT, function (Packets\UnsubscribeAck $ackPacket) use ($stream, $deferred, $unsubscribePacket) {
            if ($unsubscribePacket->getPacketId() === $ackPacket->getPacketId()) {
                $this->logger->debug('Unsubscription successful', [
                    'topic' => $unsubscribePacket->getTopic()
                ]);
                $deferred->resolve($stream);
            } else {
                $deferred->reject('Subscription ack has wrong packetId');
            }
            $deferred->resolve($stream);
        });

        return $deferred->promise();
    }

    public function publish(
        ConnectionInterface $stream,
        string $topic,
        string $message,
        int $qos = 0,
        bool $dup = false,
        bool $retain = false
    ): PromiseInterface {
        if ($this->state !== self::STATE_CONNECTED) {
            return reject('Connection unavailable');
        }

        $publishPacket = new Packets\Publish($this->version);
        $publishPacket->setTopic($topic);
        $publishPacket->setQos($qos);
        $publishPacket->setDup($dup);
        $publishPacket->setRetain($retain);

        $success = $this->sendPacketToStream($stream, $publishPacket, $message);

        $deferred = new Deferred();
        if ($success) {
            if ($qos === Packets\QoS\Levels::AT_LEAST_ONCE_DELIVERY) {
                $stream->on(Packets\PublishAck::EVENT, function (Packets\PublishAck $message) use ($deferred, $stream) {
                    $this->logger->debug('QoS: ' . Packets\QoS\Levels::AT_LEAST_ONCE_DELIVERY . ', packetId: ' . $message->getPacketId());
                    $deferred->resolve($stream);
                });
            } elseif ($qos === Packets\QoS\Levels::EXACTLY_ONCE_DELIVERY) {
                $stream->on(Packets\PublishReceived::EVENT, function (Packets\PublishReceived $receivedPacket) use ($stream, $deferred, $publishPacket) {
                    if ($publishPacket->getPacketId() === $receivedPacket->getPacketId()) {
                        $this->logger->debug('QoS: ' . Packets\QoS\Levels::AT_LEAST_ONCE_DELIVERY . ', packetId: ' . $receivedPacket->getPacketId());

                        $releasePacket = new Packets\PublishRelease($this->version);
                        $releasePacket->setPacketId($receivedPacket->getPacketId());
                        $stream->write($releasePacket->get());

                        $deferred->resolve($stream);
                    } else {
                        $deferred->reject('PublishReceived ack has wrong packetId');
                    }
                });
            } else {
                $deferred->resolve($stream);
            }
        } else {
            $deferred->reject();
        }

        return $deferred->promise();
    }

    public function disconnect(ConnectionInterface $stream): PromiseInterface
    {
        $packet = new Packets\Disconnect($this->version);
        $this->sendPacketToStream($stream, $packet);

        return resolve($stream);
    }

    protected function sendPacketToStream(
        ConnectionInterface $stream,
        Packets\ControlPacket $controlPacket,
        string $additionalPayload = ''
    ): bool {
        $this->logger->debug('Send packet to stream', ['packet' => get_class($controlPacket)]);
        $message = $controlPacket->get($additionalPayload);

        return $stream->write($message);
    }


    /**
     * Returns default connection options
     *
     * @return ConnectionOptions
     */
    private function getDefaultConnectionOptions(): ConnectionOptions
    {
        return new ConnectionOptions();
    }
}
