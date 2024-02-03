<?php
ini_set('max_execution_time', '0');

header('Access-Control-Allow-Origin: *');

ini_set('display_errors', 1);
ini_set('memory_limit', '-1'); //Unlimited memory usage - as large chunks of data may be processed

error_reporting(E_ALL);

include "Interfaces/Loader.php";
include "Traits/LogTrait.php";
include "Traits/CountersTrait.php";
include "Traits/LoaderCommonsTrait.php";

require_once __DIR__ . '/vendor/autoload.php';

use Dotenv\Dotenv;

$dotenv = Dotenv::createImmutable(__DIR__);
$dotenv->load();

$config = [
    'host'    => $_ENV['DB_HOST'],
    'user'    => $_ENV['DB_USER'],
    'pass'    => $_ENV['DB_PASS'],
    'db'      => $_ENV['DB_NAME'],

    //new RDS parameters
    'rdsLogFilePath' => $_ENV['RDS_LOG_FILE'] ?? './rds.log',
    'rdsQueryLogsEnabled' => $_ENV['RDS_LOGS_ENABLED'] ?? false,
    'rdsInsertChunkSize' => $_ENV['RDS_INSERT_CHUNK_SIZE'],

    'fm_db'   => $_ENV['FM_DB_NAME'],
    'X-FM-Server-Version' => $_ENV['FM_SERVER_VERSION'],
    'fm_host' => $_ENV['FM_HOST'],
    'fm_user' => $_ENV['FM_USER'] ?? 'realOne',
    'fm_password' => $_ENV['FM_PASSWORD'] ?? 'realOne@123',
    'aws' => [
        'region'  => $_ENV['AWS_REGION'],
        'version' => $_ENV['AWS_REGION'],
        'key'     => $_ENV['AWS_KEY'],
        'secret'  => $_ENV['AWS_SECRET'],
    ],
    'snowflake' => [
        'warehouse'    => $_ENV['SNOWFLAKE_WAREHOUSE'],
        'schema'       => $_ENV['SNOWFLAKE_SCHEMA'],
        'database'     => $_ENV['SNOWFLAKE_DATABASE'],
        'account'      => $_ENV['SNOWFLAKE_ACCOUNT'],
        'user'         => $_ENV['SNOWFLAKE_USER'],
        'password'     => $_ENV['SNOWFLAKE_PASSWORD'],
        'log_file'     => $_ENV['SNOWFLAKE_LOG_FILE'],
        'logs_enabled' => $_ENV['SNOWFLAKE_LOGS_ENABLED'] ?? false,
        'insertChunkSize' => $_ENV['SNOWFLAKE_INSERT_CHUNK_SIZE'],
    ],
    'mock' => [
        'host' => $_ENV['FM_MOCK_HOST'] ?? 'localhost',
        'database' => $_ENV['FM_MOCK_DATABASE'] ?? 'datawarehouse',
        'username' => $_ENV['FM_MOCK_USER'] ?? 'root',
        'password' => $_ENV['FM_MOCK_PASSWORD'] ?? '',
        'port' => $_ENV['FM_MOCK_PORT'] ?? 3306,
      ]
];
