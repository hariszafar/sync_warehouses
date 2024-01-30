<?php
// ini_set('display_errors', 1);
// error_reporting(E_ALL);
//   $account = "hndwrqr-sqb80692";
//   $user = "hariszafar15985";
//   $password = "Hariszafar@78


// define('SNOWFLAKE_WAREHOUSE', "COMPUTE_WH");
// define('SNOWFLAKE_SCHEMA', "PDW");
// define('SNOWFLAKE_DATABASE', "DATAWAREHOUSE");
// define('SNOWFLAKE_ACCOUNT', "IGB05606");
// define('SNOWFLAKE_USER', "BENMARCHBANKS");
// define('SNOWFLAKE_PASSWORD', "tempi5OUTMODED*crump");


if (!isset($config['snowflake']) || !isset($config['snowflake']['warehouse'])) {
    include __DIR__ . '/config.php';
}
/* 'snowflake' => [
    'warehouse' => $_ENV['SNOWFLAKE_WAREHOUSE'],
    'schema' => $_ENV['SNOWFLAKE_SCHEMA'],
    'database' => $_ENV['SNOWFLAKE_DATABASE'],
    'account' => $_ENV['SNOWFLAKE_ACCOUNT'],
    'user' => $_ENV['SNOWFLAKE_USER'],
    'password' => $_ENV['SNOWFLAKE_PASSWORD'],
    'log_file' => $_ENV['SNOWFLAKE_LOG_FILE'],
    'logs_enabled' => $_ENV['SNOWFLAKE_LOGS_ENABLED'],
], */
define('SNOWFLAKE_WAREHOUSE', $config['snowflake']['warehouse']);
define('SNOWFLAKE_SCHEMA', $config['snowflake']['schema']);
define('SNOWFLAKE_DATABASE', $config['snowflake']['database']);
define('SNOWFLAKE_ACCOUNT', $config['snowflake']['account']);
define('SNOWFLAKE_USER', $config['snowflake']['user']);
define('SNOWFLAKE_PASSWORD', $config['snowflake']['password']); 

// Logging related settings
define('SNOWFLAKE_LOG_FILE', $config['snowflake']['log_file']);
define('SNOWFLAKE_LOGS_ENABLED', $config['snowflake']['logs_enabled']);
define('SNOWFLAKE_INSERT_CHUNK_SIZE', $config['snowflake']['insertChunkSize']);
