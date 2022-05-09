<?php declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

require __DIR__ . '/vendor/autoload.php';

$stopwatch = new Stopwatch();
$stopwatch->start();

(new Flow())
    ->read(CSV::from_file(__DIR__ . '/data/dataset.csv', 1000, 0))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(new JsonLoader(__DIR__ . '/output/dataset.json'))
    ->run();

$stopwatch->stop();

echo "Total reading Parquet: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n\n";
