<?php declare(strict_types=1);

use Flow\ETL\DSL\Json;
use Flow\ETL\DSL\Stream;
use Flow\ETL\DSL\Transform;
use Flow\ETL\Flow;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/vendor/autoload.php';

$dotenv = new Dotenv();
$dotenv->load(__DIR__.'/.env');

$s3_client_option = [
    'credentials' => [
        'key' => $_ENV['S3_KEY'],
        'secret' => $_ENV['S3_SECRET'],
    ],
    'region' => 'eu-west-2',
    'version' => 'latest',
];

$blob_account = $_ENV['AZURE_BLOB_ACCOUNT'];
$blob_key = $_ENV['AZURE_BLOB_KEY'];

$azure_blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=$blob_account;AccountKey=$blob_key";

require __DIR__ . '/vendor/autoload.php';

(new Flow())
    ->read(Json::from(Stream::aws_s3_file('flow-php', 'dataset.json', $s3_client_option), 10))
    ->rows(Transform::array_unpack('row'))
    ->drop('row')
    ->rows(Transform::to_integer('id'))
    ->rows(Transform::string_concat(['name', 'last name'], ' ', 'name'))
    ->drop('last name')
    ->write(Json::to(Stream::azure_blob_file('flow-php', 'dataset.json', $azure_blob_connection_string), false))
    ->run();
