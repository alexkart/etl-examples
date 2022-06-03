<?php

use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\Stream;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;

require __DIR__ . '/../vendor/autoload.php';

(new Flow())
    ->read(CSV::from(Stream::local_file(__DIR__ . '/issue289.csv')))
    ->rows(Transform::array_unpack('row'))
    ->drop("row")
    ->write(CSV::to(Stream::local_file(__DIR__ . '/issue289_new.csv'), true, false, ',', "'"))
    ->run();
