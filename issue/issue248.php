<?php

use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

require __DIR__ . '/../vendor/autoload.php';

(new Flow())
    ->read(From::array(
        [
            ['entry_a' => 'value', 'entry_b' => 'value'],
            ['entry_a' => 'value', 'entry_b' => 'value']
        ]
    ))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(To::stdout())
    ->rows(Transform::add_integer('extra_value', \random_int(1, 5)))
    ->write(To::stdout())
    ->run();

(new Flow())
    ->read(From::array(
        [
            ['entry_a' => 'value', 'entry_b' => 'value'],
            ['entry_a' => 'value', 'entry_b' => 'value']
        ]
    ))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(To::stdout())
    ->map(static fn (Row $row) : Row => $row->add(Entry::integer('extra_value', \random_int(1, 5))))
    ->write(To::stdout())
    ->run();

(new Flow())
    ->read(From::array(
        [
            ['entry_a' => 'value', 'entry_b' => 'value'],
            ['entry_a' => 'value', 'entry_b' => 'value']
        ]
    ))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->write(To::stdout())
    ->transform(
        new class implements Transformer {
            public function transform(Rows $rows): Rows
            {
                return $rows->map(function (Row $row) : Row {
                    return $row->add(Entry::integer('extra_value', \random_int(1, 10)));
                });
            }

            public function __unserialize(array $data): void
            {
            }

            public function __serialize(): array
            {
                return [];
            }
        }
    )
    ->write(To::stdout())
    ->run();