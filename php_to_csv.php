<?php

declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Rows;

require __DIR__ . '/vendor/autoload.php';

if (\file_exists(__DIR__ . '/output/dataset_10k.parquet')) {
    \unlink(__DIR__ . '/output/dataset_10k.parquet');
}

$extractor = require __DIR__ . '/data/extractor.php';

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;
$memory = new Consumption();
$memory->current();

(new Flow())
    ->read($extractor)
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(To::callback(function (Rows $rows) use (&$total, $memory) : void {
        $total += $rows->count();

        $memory->current();
    }))
    ->write(CSV::to(__DIR__ . '/output/dataset.csv', safe_mode: true))
    ->run();

$memory->current();
$stopwatch->stop();

echo "Memory consumption, max: {$memory->max()->inMb()}Mb\n";
echo "Total writing CSV: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
