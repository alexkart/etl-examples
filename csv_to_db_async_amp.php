#!/usr/bin/env php
<?php

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Async\Amp\Server\SocketServer;
use Flow\ETL\Async\Amp\Worker\ChildProcessLauncher;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Pipeline\LocalSocketPipeline;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;

require __DIR__ . '/vendor/autoload.php';

if (!\is_dir(__DIR__ . '/var')) {
    \mkdir(__DIR__ . '/var');
}

if (!\is_dir(__DIR__ . '/var/run/')) {
    \mkdir(__DIR__ . '/var/run/');
}

$dbConnection = require __DIR__ . '/db_clean.php';

$logger = new Logger('server');
//$logger->pushHandler(new StreamHandler('php://stdout', LogLevel::DEBUG, false));
$logger->pushHandler(new StreamHandler('php://stderr', LogLevel::ERROR, false));

$csvFileSize = \round(\filesize(__DIR__ . '/data/dataset.csv') / 1024 / 1024);
echo "Loading CSV {$csvFileSize}Mb file into postgresql...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();

(new Flow())
    ->read(CSV::from_file($path = __DIR__ . '/data/dataset.csv', 10_000, 0))
    ->pipeline(
        new LocalSocketPipeline(
            SocketServer::unixDomain(__DIR__ . '/var/run/', $logger),
            ////SocketServer::tcp(6651, $logger),
            new ChildProcessLauncher(__DIR__ . '/vendor/bin/worker-amp', $logger),
            $workers = 8
        )
    )
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->rows(Transform::to_integer('id'))
    ->rows(Transform::string_concat(['name', 'last name'], ' ', 'name'))
    ->drop('last name')
    ->load(DbalLoader::fromConnection($dbConnection, 'flow_dataset_table', 1000))
    ->run();

$stopwatch->stop();

echo 'Flow PHP - Elapsed time: ' . $stopwatch->totalElapsedTime()->inSecondsPrecise() . "s \n";
$dbRows = \current($dbConnection->executeQuery('SELECT COUNT(*) FROM flow_dataset_table')->fetchNumeric());
echo "Total inserted rows: {$dbRows}\n";
