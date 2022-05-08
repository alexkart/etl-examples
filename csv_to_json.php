<?php declare(strict_types=1);

use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Loader\StreamLoader\Output;

require __DIR__ . '/vendor/autoload.php';

(new Flow())
    ->read(CSV::from_file(__DIR__ . '/data/music-scales.csv', 10, 0))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->rows(Transform::to_integer('root', 'column_c', 'column_d', 'column_e', 'column_f'))
    ->write(To::stdout($truncate = 0, $output = Output::rows_and_schema))
    ->write(new JsonLoader(__DIR__ . '/output/music-scales.json'))
    ->run();
