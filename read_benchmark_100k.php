<?php

declare(strict_types=1);

/**
 * Compare reading CSV, Parquet files, 100k rows in batch.
 */

use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Rows;

require __DIR__ . '/vendor/autoload.php';

// CVS

$csvFileSize = \round(\filesize(__DIR__ . '/data/dataset.csv') / 1024 / 1024);
echo "Reading CSV file: {$csvFileSize}Mb...\n";
$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;
$memory = new Consumption();
$memory->current();

(new Flow())
    ->read(CSV::from(__DIR__ . '/data/dataset.csv', 100_000))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(To::callback(function (Rows $rows) use (&$total, $memory) : void {
        $total += $rows->count();
        $memory->current();
    }))
    ->run();

$memory->current();
$stopwatch->stop();

echo "Read rows: {$total}\n";
echo "Memory consumption, max: {$memory->maxDiff()->inMb()}Mb\n";
echo "Total reading CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";

// PARQUET

$parquetFileSize = \round(\filesize(__DIR__ . '/data/dataset_100k.parquet') / 1024 / 1024);
echo "Reading Parquet file: {$parquetFileSize}Mb...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;
$memory = new Consumption();
$memory->current();

(new Flow())
    ->read(Parquet::from(__DIR__ . '/data/dataset_100k.parquet'))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(To::callback(function (Rows $rows) use (&$total, $memory) : void {
        $total += $rows->count();
        $memory->current();
    }))
    ->run();

$memory->current();
$stopwatch->stop();

echo "Read rows: {$total}\n";
echo "Memory consumption, max: {$memory->maxDiff()->inMb()}Mb\n";
echo "Total reading Parquet: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
