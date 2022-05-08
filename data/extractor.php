<?php declare(strict_types=1);

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Rows;

return new class implements Extractor {
    public function extract() : Generator
    {
        $rows = [];

        for ($i = 0; $i <= 2_000_000; $i++) {
            $rows[] = Row::create(
                new ArrayEntry(
                    'row',
                    ['id' => $i, 'name' => 'Name', 'last name' => 'Last Name', 'phone' => '123 123 123']
                ),
            );

            if (\count($rows) >= 100_000) {
                print 'extracted ' . $i . "\n";
                yield new Rows(...$rows);

                $rows = [];
            }
        }

        if (\count($rows) >= 0) {
            yield new Rows(...$rows);
        }
    }
};