<?php

/********************************************
 * Author: Haris Zafar
 * 
 * This script queries the FileMaker server, and creates a schema in the targetted data warehouse
 * The schema is created based on the tables in the FileMaker server, and the mapping of the
 * fields in the FileMaker server to the fields in the Snowflake data warehouse.
 * The script can be run from the command line, or from a web server.
 * 
 * The script can be run with the following parameters:
 * 'tables' => a comma separated list of tables to be updated in the data warehouse
 * 'logging_off' => a flag to turn off logging
 * 'verbose' => a flag to turn on verbose logging
 * 'ignore' => a comma separated list of tables to ignore
 * 'debug_logging' => a flag to turn on debug logging
 * 'drop_create' => a flag to drop and recreate the table in the datawarehouse
 * 'backup_tables' => a flag to backup the previous table in the datawarehouse by renaming it
 * 'local_testing' => a flag indicating the script is being run in a local testing environment,
 * which utilizes a local MySQL database as a substitute/mock for the FileMaker server
 * 
 ***********************************************/

// for command line use the session array (which is undefined at this point)
// # php snowflakeSchemaBuilder.php  --drop_create=1 --backup_tables=1 --verbose=1


$targets = [];
if (defined('PHP_SAPI') && 'cli' === PHP_SAPI) {
    $GLOBALS['_SESSION'] = [];
    // get options eg ETL.php --type therapist - [returns an array of options]
    $options = getopt(null ?? '', ['tables:', 'logging_off:', 'verbose:',
        'ignore:', 'debug_logging:', 'local_testing:', 'drop_create:', 'backup_tables:']);
} elseif (session_status() == PHP_SESSION_NONE) { // uses session to store data api token
    session_start();
    header("Access-Control-Allow-Origin: *");
}
$localTesting = (isset($options['local_testing']) || isset($_REQUEST['local_testing'])) ?? false;

require_once(__DIR__ . DIRECTORY_SEPARATOR . 'config.php');
if (!$localTesting) {
    require_once(__DIR__ . DIRECTORY_SEPARATOR . 'FM_extract.php');
} else {
    //This is actually a fallback to a local MySQL database
    require_once(__DIR__ . DIRECTORY_SEPARATOR . 'lib' . DIRECTORY_SEPARATOR
        . 'mock' . DIRECTORY_SEPARATOR . 'FM_extract.php');
}

require_once(__DIR__ . DIRECTORY_SEPARATOR . 'snowflakeLoader.php');

$createdTables = [];
$backupTableNames = [];

$verboseLogging = (isset($options['verbose']) || isset($_REQUEST['verbose'])) ?? false;
if ($verboseLogging) {
    $verboseLogLevel = ((int) ($options['verbose'] ?? $_REQUEST['verbose']) > 0) ?
        (int) ($options['verbose'] ?? $_REQUEST['verbose']) : 1;
} else {
    $verboseLogLevel = 1;
}

if (isset($options['drop_create']) || isset($_REQUEST['drop_create']) ) {
    $dropCreateValue = (int)($options['drop_create'] ?? $_REQUEST['drop_create']);
    $dropCreate = ($dropCreateValue > 0);
} else {
    $dropCreate = false;
}

if (isset($options['backup_tables']) || isset($_REQUEST['backup_tables'])) {
    $backupTablesValue = (int)($options['backup_tables'] ?? $_REQUEST['backup_tables']);
    $backupTables = ($backupTablesValue > 0);
} else {
    $backupTables = false;
}

// list of tables to ignore
$ignoreTables = (isset($options['ignore']) || isset($_REQUEST['ignore'])) ?
    (explode(",", $options['ignore']) ?? explode(",", $_REQUEST['ignore'])) : [];

//turn all elements of ignoreTables to lowercase
$ignoreTables = array_map('strtolower', $ignoreTables);

//list of tables in Data Warehouse to be updated.
$tablesParam = (isset($options['tables']) || isset($_REQUEST['tables'])) ?
    (explode(",", $options['tables']) ?? explode(",", $_REQUEST['tables'])) : [];
$tablesParam = array_map('trim', $tablesParam);
$defaultTablesList = require(__DIR__ . DIRECTORY_SEPARATOR . 'config'
    . DIRECTORY_SEPARATOR . 'destTablesList.php');
$dest_table_list = (!empty($tablesParam)) ? $tablesParam : $defaultTablesList;

//map primary keys with the respective tables
$primaryKeyPairs = require(__DIR__ . DIRECTORY_SEPARATOR . 'config'
    . DIRECTORY_SEPARATOR . 'primaryKeyPairs.php');

// Map the source table names (keys) to the search query (values)
$sourceSearchQuery = require(__DIR__ . DIRECTORY_SEPARATOR . 'config'
    . DIRECTORY_SEPARATOR . 'sourceSearchQueries.php');

// valid search timestamp column names
$validTimestampColumns = require(__DIR__ . DIRECTORY_SEPARATOR . 'config'
    . DIRECTORY_SEPARATOR . 'validTimestampColumns.php');

// Map the destination (Data Warehouse) table names (keys)
// to the source (FileMaker) table names (values)
$tablesMap = require(__DIR__ . DIRECTORY_SEPARATOR . 'config'
    . DIRECTORY_SEPARATOR . 'tablesMap.php');

// A map of source table names (keys) to the column names
// in the destination table (Data Warehouse) (values)
$tableColumnMaps = require(__DIR__ . DIRECTORY_SEPARATOR . 'config'
    . DIRECTORY_SEPARATOR . 'tableColumnMaps.php');

$source_list = [];

// default first date time to '01/01/1970 00:00:00',
// as we will only be fetching one record from the source table
$daysOldSet = true;
$defaultFirstDateTime = $last_update = '01/01/1970 00:00:00';

$logging =  isset($options['logging_off']) ? 0 : 1;
$debug_logging = (isset($options['debug_logging']) && $options['debug_logging'] == 0) ? 0 : 1;
$db = $config['fm_db']; // database name on FileMaker server
$host = $config['fm_host']; // ip address of server

//fallback configuration for local MySQL database to act as substitute for FileMaker
if ($localTesting) {
    $host = $config['mock']['host'];
    $db = [
        'database' => $config['mock']['database'],
        'username' => $config['mock']['username'],
        'password' => $config['mock']['password']
    ];
}

$limit = 1; // limit the number of records to be fetched from the source table to 1

$snow = null;
try {
    //code...
    $snow = new SnowflakeLoader($config['snowflake']);
    $snow->setVerboseExecutionLogging($verboseLogging);
    if ($verboseLogging) {
        $snow->setVerboseLogLevel($verboseLogLevel);
    }
    $snow->clearLogFile(); // initialize log file
    $snow->setDebugLogging($debug_logging);
} catch (\Throwable $th) {
    die("Error: Unable to establish connection with Snowflake. [" . $th->getCode()
        . "]: " . $th->getMessage());
}

$log = [];
$total_records = 0;
$timeSort = ["fieldName" => "modificationHostTimestamp", "sortOrder" => "ascend"];
$starttime = microtime(true);
$rdsTimeLapse = [];
$snowflakeTimeLapse = [];
//$log[] = "Sync records modified date ".$last_update;
//$log[] = "-------------------------------------------------------";

// Include the functions file, containing the ETL helper functions
include_once(__DIR__ . DIRECTORY_SEPARATOR . 'functions.php');

// Let's sync the data for each table (or each requested/targeted table)
foreach ($tablesMap as $dest_table => $source_table)
{
    if (in_array($dest_table, $dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
        $search = null;
        $isTimestampTable = searchContainsTimestamps($sourceSearchQuery[$dest_table] ?? []);
        // $sort = ($isTimestampTable) ? getTimeSort($dest_table) : null;
        $sort = null; // no sorting
        // prepare search
        $search = prepareSearch($snow, $daysOldSet, $daysOffset ?? null, $last_update ?? $defaultFirstDateTime, $dest_table, $sourceSearchQuery[$dest_table] ?? null);
        
        $offset = 0;
        $column_map = $tableColumnMaps[$dest_table] ?? [];

        // $search variable will be set above, in case of $extractOnce = true
        // $fquery = new FM_extract($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map, true); //use with local_testing only
        $fquery = new FM_extract($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map);
        $fquery->offset = 0;
        $update_res =  [];

        //fetch only once
        $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
        if ($dest_table == 'narrative_report') {
            $nrs = [];
            $rs_array = json_decode($rs ?? '');
            if (is_array($rs_array)) {
                foreach ($rs_array as $nrec) {
                    $nt = $nrec->text_upper;
                    $hcpcs = extractHCPCS($nt);
                    $nrec->hcpcs = $hcpcs;
                    $nrs[] = $nrec;
                }
                $rs = json_encode($nrs, true); //update the rs variable with the new data
            }
        } else {
            $rs_array = json_decode($rs ?? '', true);
        }

        $primaryKey = $primaryKeyPairs[$dest_table]; // Provide Primary Key for table
        
        // For the script to determine the schema, it's essential that a record is fetched from the source table
        if ((count((array)$rs_array) > 0)) {
            $begin = microtime(true);
            $createdTables[$dest_table] = $snow->createTableSchemaFromData($rs, $dest_table, $primaryKey, $dropCreate, $backupTables);
            if ($createdTables[$dest_table] && $backupTables) {
                $backupTableNames[$dest_table] = $snow->getBackupTableName($dest_table);
            }
        }

        // Snowflake Logs
        // if (isset($snowflakeTimeLapse[$dest_table]) && $snowflakeTimeLapse[$dest_table] > 0) {
        //     if ($snowflakeAffectedRows[$dest_table] > 0) {
        //         $logRecord = [$dest_table => $snowflakeAffectedRows[$dest_table]];
        //         snowEtlLogUpdate($snow, $logRecord, $snowflakeTimeLapse[$dest_table], $snowLastModifiedTimestamp[$dest_table]);
        //     }
        // }

        //since a new instance of FM_extract is created for each table, we need to unset the variable to free up memory
        unset($fquery);
    }
}

// ********************* end of tables to sync ********************


// ************* Display script log *************

$endtime = microtime(true);
$timediff = number_format($endtime - $starttime, 2);

// $update_date = date_create($last_update ?? );
// $last_update_formatted =   date_format($update_date, 'Y-m-d');

// Snowflake Logs
// were any tables created
if (count($createdTables) > 0)  {
    echo PHP_EOL;
    print_r("Snowflake Tables created:");
    echo PHP_EOL;
    print_r($createdTables);
    if ($backupTables) {
        echo PHP_EOL;
        print_r("Backup Table Names:");
        echo PHP_EOL;
        print_r($backupTableNames);
    }
} else {
    echo PHP_EOL;
    print_r("No Snowflake Tables created");
}

print_r("\n");
print_r("Total timelapse in seconds: " . $timediff . " s");
