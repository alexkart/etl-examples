<?php declare(strict_types=1);

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Consumption;
use Flow\ETL\Rows;

require __DIR__ . '/vendor/autoload.php';

if (\file_exists(__DIR__ . '/output/dataset.json')) {
    \unlink(__DIR__ . '/output/dataset.json');
}

$csvFileSize = \round(\filesize(__DIR__ . '/data/dataset.csv') / 1024 / 1024);
echo "Converting CSV {$csvFileSize}Mb file into json...\n";

$stopwatch = new Stopwatch();
$stopwatch->start();
$total = 0;
$memory = new Consumption();
$memory->current();

(new Flow())
    ->read(CSV::from_file(__DIR__ . '/data/dataset.csv', 1000, 0))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(To::callback(function (Rows $rows) use (&$total, $memory) : void {
        $total += $rows->count();

        $memory->current();
    }))
    ->write(new JsonLoader(__DIR__ . '/output/dataset.json'))
    ->run();

$memory->current();
$stopwatch->stop();

echo "Memory consumption, max: {$memory->max()->inMb()}Mb\n";
echo "Total elapsed time: {$stopwatch->totalElapsedTime()->inSecondsPrecise()}s\n";

$jsonFileSize = \round(\filesize(__DIR__ . '/output/dataset.json') / 1024 / 1024);
echo "Output JSON file size: {$jsonFileSize}Mb\n";
