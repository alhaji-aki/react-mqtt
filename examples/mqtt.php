<?php

use Morbo\React\Mqtt\Client;
use Morbo\React\Mqtt\ConnectionOptions;
use Morbo\React\Mqtt\Protocols\Version4;

require_once __DIR__ . '/../vendor/autoload.php';

$loop = React\EventLoop\Factory::create();

$config = [
    'host' => 'localhost',
    'port' => 1883,
    'options' => new ConnectionOptions()
];

$mqtt = new Client($loop, new Version4());
