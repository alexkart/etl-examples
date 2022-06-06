
<?php

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

require __DIR__ . '/vendor/autoload.php';

if (!\is_dir(__DIR__ . '/var')) {
    \mkdir(__DIR__ . '/var');
}

if (!\is_dir(__DIR__ . '/var/run/')) {
    \mkdir(__DIR__ . '/var/run/');
}

// source db connection
echo "Loading source data into postgresql...\n";
$sourceDbConnection = require __DIR__ . '/db_source.php';

// target db connection
$dbConnection = require __DIR__ . '/db_clean.php';

$logger = new Logger('server');
$logger->pushHandler(new StreamHandler(__DIR__ . '/../var/logs/server.log', Logger::DEBUG));
$logger->pushHandler(new StreamHandler(__DIR__ . '/../var/logs/server_error.log', Logger::ERROR, false));

$stopwatch = new Stopwatch();
$stopwatch->start();

$rows = 1000;
echo "Loading $rows rows into postgresql...\n";

(new Flow())
    ->read(new DbalQueryExtractor($sourceDbConnection, "SELECT * FROM source_dataset_table LIMIT $rows;"))
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
