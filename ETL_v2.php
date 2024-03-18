<?php

/********************************************
Script modified by Haris Zafar
- Implemented DRY principles
based off of ETL.php created by Ben Marchbanks

This script performs a query in FileMaker Database for each table in the Data Warehouse.
It queries for modified records since the last sync and transforms then loads
them into their respective table in the Data Warehouse.

Once the sycn is complete it reports the results into the table "etl_log"
in the Data Warehouse. This log records a summary of the changed records
and could be useful in a visualization to see key activities taking place accross
the enterprise.

Notes
    1. Date format in FileMaker is d/m/y - transformed to Y-m-d
    2. Data from relationships includes long crypted names for the columns
      these are mapped to shorter column names for readbility
    3. Layouts in FM used for queries should be named table name+ "_data_warehouse"

 ***********************************************/

// for command line use the session array (which is undefined at this point)
// C:/scripts/sync/ETL_v2.php --days_old=1000 --tables=invoice_items --target=RDS,SNOWFLAKE --verbose=1


$targetedExecution = false; // flag to check whether a specific script has been targeted
$snowflakeTargeted = false; // flag to check whether snowflake has been targeted
$rdsTargeted = false; //flag to check whether rds has been targeted
$targets = [];

$defaultTablesList = require(__DIR__ . DIRECTORY_SEPARATOR . 'config'
    . DIRECTORY_SEPARATOR . 'destTablesList.php');

// create an array of table names with the chunk size option
$tableChunkSizeOptions = array_map( function($tableName) {
    return $tableName . '_chunk_size' . ':' ;
}, $defaultTablesList);

if (defined('PHP_SAPI') && 'cli' === PHP_SAPI) {
    $GLOBALS['_SESSION'] = [];
    // get options eg ETL.php --type therapist - [returns an array of options]
    $options = getopt(null ?? '', array_merge(['tables:', 'logging_off:', 'verbose:',
        'ignore:', 'debug_logging:', 'local_testing:', 'days_old:', 'target:',
        'force_chunk_size:'], $tableChunkSizeOptions));
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

//Check if only specific destination (RDS or Snowflake or both) have been targeted
if (!empty($options['target'])) {
    $targets = explode(",", strtolower($options['target']));
} else if (!empty($_REQUEST['target'])) {
    $targets = explode(",", strtolower($_REQUEST['target']));
} else {
    $targets = ['snowflake', 'rds']; //since none have been targeted, both will be executed
}


if (in_array('snowflake', $targets)) {
    require_once(__DIR__ . DIRECTORY_SEPARATOR . 'snowflakeLoader.php');
    $targetedExecution = true;
    $snowflakeTargeted = true;
    $snowflakeAffectedRows = [];
}
if (in_array('rds', $targets)) {
    require_once(__DIR__ . DIRECTORY_SEPARATOR . 'RDS_load.php');
    $targetedExecution = true;
    $rdsTargeted = true;
}

$verboseLogging = (isset($options['verbose']) || isset($_REQUEST['verbose'])) ?? false;
if ($verboseLogging) {
    $verboseLogLevel = ((int) ($options['verbose'] ?? $_REQUEST['verbose']) > 0) ?
        (int) ($options['verbose'] ?? $_REQUEST['verbose']) : 1;
} else {
    $verboseLogLevel = 1;
}
//require_once(__DIR__ . DIRECTORY_SEPARATOR . '/lib/CloudWatchLogger.php');

// list of tables to ignore
$ignoreTables = (isset($options['ignore']) || isset($_REQUEST['ignore'])) ?
    (explode(",", $options['ignore'] ?? $_REQUEST['ignore'])) : [];

//turn all elements of ignoreTables to lowercase
$ignoreTables = array_map('strtolower', $ignoreTables);

//list of tables in Data Warehouse to be updated.
$tablesParam = (isset($options['tables']) || isset($_REQUEST['tables'])) ?
    (explode(",", $options['tables'] ?? $_REQUEST['tables'])) : [];
$tablesParam = array_map('trim', $tablesParam);

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

// Tables that require the subtraction of days from the last modified date
$daySubtractionTables = require(__DIR__ . DIRECTORY_SEPARATOR . 'config'
    . DIRECTORY_SEPARATOR . 'daySubtractionTables.php');

$source_list = [];

// default first date time in case the last record's modified date can't be retrieved from the etl_log table
$defaultFirstDateTime = '01/01/1970 00:00:00';

if (isset($options['days_old'])) {
    $daysOldSet = true;
    // $days_old = isset($options['days_old'])? $options['days_old'] : 5; // fallback value here is pointless
    $days_old = $options['days_old'];
    $last_update = date("m/d/Y H:i:s", strtotime('-' . $days_old . ' days'));
    $daysOffset = null;
    // $yesterdays = date("m/d/Y H:i:s", strtotime('-' . $days_old . ' days'));
} else {
    $daysOldSet = false;
    $daysOffset = 20; //default value of 20 days for each sync iteration
}

if (isset($options['force_chunk_size']) || isset($_REQUEST['force_chunk_size'])) {
    $forceChunkSize = (int) ($options['force_chunk_size'] ?? $_REQUEST['force_chunk_size']);
} else {
    $forceChunkSize = null;
}
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

// limit the number of records to be fetched from the source table in one iterative go
$limit = $forceChunkSize ?? $config['fm_fetch_limit'];
$defaultSnowflakeInsertChunkSize = 500; // default insert chunk size for snowflake

if (!$targetedExecution || $rdsTargeted) {
    $rds = new RDS_load($config);
    $rds->setVerboseExecutionLogging($verboseLogging);
    if ($verboseLogging) {
        $rds->setVerboseLogLevel($verboseLogLevel);
    }
    $rds->clearLogFile(); // initialize log file
    $rds->setDebugLogging($debug_logging);
}
if (!$targetedExecution || $snowflakeTargeted) {
    try {
        //code...
        $snow = new SnowflakeLoader($config['snowflake']);
        $snow->setVerboseExecutionLogging($verboseLogging);
        if ($verboseLogging) {
            $snow->setVerboseLogLevel($verboseLogLevel);
        }
        $snow->clearLogFile(); // initialize log file
        $snow->setDebugLogging($debug_logging);
        $snow->setInsertChunkSize(
            $forceChunkSize ?? $config['snowflake']['insertChunkSize']
            ?? $defaultSnowflakeInsertChunkSize
        ); // set the insert chunk size
    } catch (\Throwable $th) {
        die("Error: Unable to establish connection with Snowflake. [" . $th->getCode()
            . "]: " . $th->getMessage());
    }
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
        $tableChunkSize = (
            !empty($options[$dest_table . '_chunk_size']) || !empty($_REQUEST[$dest_table . '_chunk_size'])
            ) ?
            (int) ($options[$dest_table . '_chunk_size'] ?? $_REQUEST[$dest_table . '_chunk_size'])
            : null;
        $limit = $tableChunkSize ?? $forceChunkSize ?? $config['fm_fetch_limit'];
        // die($dest_table . '_chunk_size: ' . $limit);
        $search = $rdsSearch = $snowflakeSearch = null;
        $isTimestampTable = searchContainsTimestamps($sourceSearchQuery[$dest_table] ?? []);
        // $sort = ($isTimestampTable) ? $timeSort : null;
        $sort = ($isTimestampTable) ? getTimeSort($dest_table) : null;
        // extract timestamp column name in a variable for later use if this is a timestamp table
        if ($isTimestampTable) {
            $timestampColumn = $sort[0]["fieldName"] ?? null;
            $mappedTimestampColumn = $column_map[$timestampColumn] ?? $timestampColumn;
        }
        if (!$targetedExecution || $rdsTargeted) {
            // $rdsSearch = ($isTimestampTable) ? $search : prepareSearch($rds, $daysOldSet, $daysOffset, $last_update, $dest_table, $sourceSearchQuery[$source_table] ?? null);
            $rdsSearch = prepareSearch($rds, $daysOldSet, $daysOffset, $last_update ?? null, $dest_table, $sourceSearchQuery[$dest_table] ?? null);
        }
        if (!$targetedExecution || $snowflakeTargeted) {
            // $snowflakeSearch = ($isTimestampTable) ? $search : prepareSearch($snow, $daysOldSet, $daysOffset, $last_update, $dest_table, $sourceSearchQuery[$source_table] ?? null);
            $snowflakeSearch = prepareSearch($snow, $daysOldSet, $daysOffset, $last_update ?? null, $dest_table, $sourceSearchQuery[$dest_table] ?? null);
            $snow->setInsertChunkSize($tableChunkSize ?? $forceChunkSize ?? $config['snowflake']['insertChunkSize'] ?? $defaultSnowflakeInsertChunkSize);
        }
        
        $extractOnce = shouldExtractOnce($rdsSearch ?? null, $snowflakeSearch ?? null);
        if ($extractOnce) {
            $search = $rdsSearch ?? $snowflakeSearch;
        }
        $offset = 0;
        $column_map = $tableColumnMaps[$dest_table] ?? [];

        if ($extractOnce) {
            // $search variable will be set above, in case of $extractOnce = true
            // $fquery = new FM_extract($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map, true); //use with local_testing only
            $fquery = new FM_extract($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map);
            $fquery->offset = 0;
            $update_res =  [];
            do {
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
                if ((!$targetedExecution || $rdsTargeted) && (count((array)$rs_array) > 0)) {
                    $begin = microtime(true);
                    $update_res[] = (count((array)$rs_array) > 0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0, 0, 0];
                    $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                    $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                    $rs_array = (!is_array($rs_array)) ? (array)$rs_array : $rs_array;
                    $mappedTimestampColumn = null;
                    if ($isTimestampTable) {
                        $mappedTimestampColumn = $column_map[$timestampColumn] ?? $timestampColumn;
                    } else {
                        foreach ($validTimestampColumns as $timestampColumn) {
                            if (array_key_exists($timestampColumn, $column_map)) {
                                $mappedTimestampColumn = $column_map[$timestampColumn];
                                break;
                            }
                        }
                    }
                    if (!empty($mappedTimestampColumn)) {
                        //parsing of last record is required in case it is an object (e.g. narrative_report)
                        $lastRecord = (!is_array($rs_array[count($rs_array) - 1])) ?
                            (array)$rs_array[count($rs_array) - 1] : $rs_array[count($rs_array) - 1];
                        $rdsLastModifiedTimestamp[$dest_table] = date_format(
                            date_create_from_format(
                                'Y-m-d H:i:s',
                                $lastRecord[$mappedTimestampColumn] ?? date('Y-m-d H:i:s')
                            ),
                            'Y-m-d H:i:s'
                        ) // date from last record - FM_extract converts it to 'Y-m-d H:i:s' while mapping and returning
                        ?? date('Y-m-d H:i:s'); // fallback to current date
                    } else {
                        $rdsLastModifiedTimestamp[$dest_table] = date('Y-m-d H:i:s');
                    }
                }
                if ((!$targetedExecution || $snowflakeTargeted) && (count((array)$rs_array) > 0)) {
                    $begin = microtime(true);
                    $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                    $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
    
                    $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                    $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);

                    $rs_array = (!is_array($rs_array)) ? (array)$rs_array : $rs_array;
                    $mappedTimestampColumn = null;
                    if ($isTimestampTable) {
                        $mappedTimestampColumn = $column_map[$timestampColumn] ?? $timestampColumn;
                    } else {
                        foreach ($validTimestampColumns as $timestampColumn) {
                            if (array_key_exists($timestampColumn, $column_map)) {
                                $mappedTimestampColumn = $column_map[$timestampColumn];
                                break;
                            }
                        }
                    }
                    if (!empty($mappedTimestampColumn)) {
                        //parsing of last record is required in case it is an object (e.g. narrative_report)
                        $lastRecord = (!is_array($rs_array[count($rs_array) - 1])) ?
                            (array)$rs_array[count($rs_array) - 1] : $rs_array[count($rs_array) - 1];
                        $snowLastModifiedTimestamp[$dest_table] = date_format(
                            date_create_from_format(
                                'Y-m-d H:i:s',
                                $lastRecord[$mappedTimestampColumn] ?? date('Y-m-d H:i:s')
                            ),
                            'Y-m-d H:i:s'
                        ) // date from last record - FM_extract converts it to 'Y-m-d H:i:s' while mapping and returning
                        ?? date('Y-m-d H:i:s'); // fallback to current date
                    } else {
                        $snowLastModifiedTimestamp[$dest_table] = date('Y-m-d H:i:s');
                    }
                }
                $offset += $limit;
                $fquery->offset = $offset;
            } while (count((array)$rs_array) > 0);
    
            // if ($isTimestampTable) {
                // if ((!$targetedExecution || $rdsTargeted) && $isTimestampTable && !$daysOldSet) {
                // if ((!$targetedExecution || $rdsTargeted) && !$daysOldSet) {
            if ((!$targetedExecution || $rdsTargeted)) {
                // update etl_log table for RDS
                if (isset($rdsTimeLapse[$dest_table]) && $rdsTimeLapse[$dest_table] > 0) {
                    $logRecord = [$dest_table, sumRes($update_res)];
                    rdsEtlLogUpdate($rds, $logRecord, $rdsTimeLapse[$dest_table], $rdsLastModifiedTimestamp[$dest_table]);
                    if (!isset($log) || !is_array($log)) {
                        $log = [];
                    }
                    $log[] = $logRecord;
                    $source_list[$source_table] = $dest_table . ":" . array_sum(sumRes($update_res));
                }
            }

            // if ((!$targetedExecution || $snowflakeTargeted) && $isTimestampTable && !$daysOldSet) {
            // if ((!$targetedExecution || $snowflakeTargeted) && !$daysOldSet) {
            if ((!$targetedExecution || $snowflakeTargeted)) {
                // Snowflake Logs
                if (isset($snowflakeTimeLapse[$dest_table]) && $snowflakeTimeLapse[$dest_table] > 0) {
                    if ($snowflakeAffectedRows[$dest_table] > 0) {
                        $logRecord = [$dest_table => $snowflakeAffectedRows[$dest_table]];
                        snowEtlLogUpdate($snow, $logRecord, $snowflakeTimeLapse[$dest_table], $snowLastModifiedTimestamp[$dest_table]);
                    }
                }
            }
            // }
    
            //since a new instance of FM_extract is created for each table, we need to unset the variable to free up memory
            unset($fquery);
        } else {
            // $search variable will not be set above, in case of $extractOnce = false
            // First, we'll extract data for RDS
            if (isset($rdsSearch)) {
                $search = $rdsSearch;
                unset($rdsSearch);
                // $fquery = new FM_extract($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map, true); //use with local_testing only
                $fquery = new FM_extract($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map);
                $fquery->offset = 0;
                $update_res =  [];
                do {
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
                    if ((!$targetedExecution || $rdsTargeted) && (count((array)$rs_array) > 0)) {
                        $begin = microtime(true);
                        $update_res[] = (count((array)$rs_array) > 0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0, 0, 0];
                        $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                        $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                        $rs_array = (!is_array($rs_array)) ? (array)$rs_array : $rs_array;
                        $mappedTimestampColumn = null;
                        if ($isTimestampTable) {
                            $mappedTimestampColumn = $column_map[$timestampColumn] ?? $timestampColumn;
                        } else {
                            foreach ($validTimestampColumns as $timestampColumn) {
                                if (array_key_exists($timestampColumn, $column_map)) {
                                    $mappedTimestampColumn = $column_map[$timestampColumn];
                                    break;
                                }
                            }
                        }
                        if (!empty($mappedTimestampColumn)) {
                            //parsing of last record is required in case it is an object (e.g. narrative_report)
                            $lastRecord = (!is_array($rs_array[count($rs_array) - 1])) ?
                                (array)$rs_array[count($rs_array) - 1] : $rs_array[count($rs_array) - 1];
                            $rdsLastModifiedTimestamp[$dest_table] = date_format(
                                date_create_from_format(
                                    'Y-m-d H:i:s',
                                    $lastRecord[$mappedTimestampColumn] ?? date('Y-m-d H:i:s')
                                ),
                                'Y-m-d H:i:s'
                            ) // date from last record - FM_extract converts it to 'Y-m-d H:i:s' while mapping and returning
                            ?? date('Y-m-d H:i:s'); // fallback to current date
                        } else {
                            $rdsLastModifiedTimestamp[$dest_table] = date('Y-m-d H:i:s');
                        }
                    }
                    $offset += $limit;
                    $fquery->offset = $offset;
                } while (count((array)$rs_array) > 0);
    
                // if ((!$targetedExecution || $rdsTargeted) && $isTimestampTable) {
                if ((!$targetedExecution || $rdsTargeted)) {
                    if (isset($rdsTimeLapse[$dest_table]) && $rdsTimeLapse[$dest_table] > 0) {
                        $logRecord = [$dest_table, sumRes($update_res)];
                        rdsEtlLogUpdate($rds, $logRecord, $rdsTimeLapse[$dest_table], $rdsLastModifiedTimestamp[$dest_table]);
                        if (!isset($log) || !is_array($log)) {
                            $log = [];
                        }
                        $log[] = $logRecord;
                        $source_list[$source_table] = $dest_table . ":" . count((array)$rs_array);
                    }
                }
                //since a new instance of FM_extract is created for each table, we need to unset the variable to free up memory
                unset($fquery);
            }
    
            // Next, we'll extract data for Snowflake
            if (isset($snowflakeSearch)) {
                $search = $snowflakeSearch;
                unset($snowflakeSearch);
                // $fquery = new FM_extract($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map, true); //use with local_testing only
                $fquery = new FM_extract($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map);
                $fquery->offset = 0;
                $update_res =  [];
                do {
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
                    if ((!$targetedExecution || $snowflakeTargeted) && (count((array)$rs_array) > 0)) {
                        $begin = microtime(true);
                        $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                        $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
    
                        $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                        $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);

                        $rs_array = (!is_array($rs_array)) ? (array)$rs_array : $rs_array;
                        $mappedTimestampColumn = null;
                        if ($isTimestampTable) {
                            $mappedTimestampColumn = $column_map[$timestampColumn] ?? $timestampColumn;
                        } else {
                            foreach ($validTimestampColumns as $timestampColumn) {
                                if (array_key_exists($timestampColumn, $column_map)) {
                                    $mappedTimestampColumn = $column_map[$timestampColumn];
                                    break;
                                }
                            }
                        }
                        if (!empty($mappedTimestampColumn)) {
                            //parsing of last record is required in case it is an object (e.g. narrative_report)
                            $lastRecord = (!is_array($rs_array[count($rs_array) - 1])) ?
                                (array)$rs_array[count($rs_array) - 1] : $rs_array[count($rs_array) - 1];
                            $snowLastModifiedTimestamp[$dest_table] = date_format(
                                date_create_from_format(
                                    'Y-m-d H:i:s',
                                    $lastRecord[$mappedTimestampColumn] ?? date('Y-m-d H:i:s')
                                ),
                                'Y-m-d H:i:s'
                            ) // date from last record - FM_extract converts it to 'Y-m-d H:i:s' while mapping and returning
                            ?? date('Y-m-d H:i:s'); // fallback to current date
                        } else {
                            $snowLastModifiedTimestamp[$dest_table] = date('Y-m-d H:i:s');
                        }
                    }
                    $offset += $limit;
                    $fquery->offset = $offset;
                } while (count((array)$rs_array) > 0);
    
                if ((!$targetedExecution || $snowflakeTargeted)) {
                    // Snowflake Logs
                    if (isset($snowflakeTimeLapse[$dest_table]) && $snowflakeTimeLapse[$dest_table] > 0) {
                        if ($snowflakeAffectedRows[$dest_table] > 0) {
                            $logRecord = [$dest_table => $snowflakeAffectedRows[$dest_table]];
                            snowEtlLogUpdate($snow, $logRecord, $snowflakeTimeLapse[$dest_table], $snowLastModifiedTimestamp[$dest_table]);
                        }
                    }
                }
                //since a new instance of FM_extract is created for each table, we need to unset the variable to free up memory
                unset($fquery);
            }
        }
    }
}

// ********************* end of tables to sync ********************


// ************* Display script log *************

$endtime = microtime(true);
$timediff = number_format($endtime - $starttime, 2);

// $update_date = date_create($last_update ?? );
// $last_update_formatted =   date_format($update_date, 'Y-m-d');

if (!$targetedExecution || $rdsTargeted) {
    //if any records were updated in RDS
    if (isset($log) && is_array($log) && isset($source_list)) {
        $nochange_records = 0;
        $change_records = 0;
        $add_records  = 0;

        foreach ($log as $res) {
            $nochange_records += $res[1][0];
            $change_records += $res[1][2];
            $add_records  += $res[1][1];
        }

        $prettySourceLog =  implode("\n", json_decode(json_encode($source_list), true));

        // since this has to be linked with each table's last modified date, we're skipping it due to an overload of information
        // print_r("\n");
        // print_r("date_modified: >" . ($rdsLastModifiedTimestamp[$dest_table] ?? $last_update));
        print_r("\n");
        print_r("RDS time elapsed in seconds: (" . array_sum($rdsTimeLapse) . " s)");
        print_r("\n");
        print_r($rdsTimeLapse);
        print_r("\n");
        print_r("records unchanged: " .  $nochange_records);
        print_r("\n");
        print_r("records updated: " .  $change_records);
        print_r("\n");
        print_r("records added: " .  $add_records);
        print_r("\n\n");
        print_r("data_lineage: " . $prettySourceLog);
    } else {
        // print_r("no RDS records added for " . ($rdsLastModifiedTimestamp[$dest_table] ?? $last_update));
        // Modifying display message, as this portion will explode with information if all tables are being synced
        print_r("no RDS records processed" . (isset($last_update) ? "for - " . $last_update : ''));
    }
}

// Snowflake Logs
if (!$targetedExecution || $snowflakeTargeted) {
    $totalAffectedRecords = array_sum($snowflakeAffectedRows);
    if ($totalAffectedRecords > 0) {
        $source_log = json_encode($snowflakeAffectedRows, true);
        $prettySourceLog =  implode("\n", json_decode($source_log, true));
        if (isset($last_update)) {
            echo PHP_EOL;
            print_r("date_modified: >" . $last_update);
        }
        echo PHP_EOL;
        print_r("Snowflake time elapsed in seconds: (" . array_sum($snowflakeTimeLapse) . " s)");
        echo PHP_EOL;
        print_r($snowflakeTimeLapse);
        echo PHP_EOL;
        print_r("Snowflake Affected rows:");
        echo PHP_EOL;
        print_r($snowflakeAffectedRows);
        echo PHP_EOL;
        print_r("Total Snowflake records affected: " .  $totalAffectedRecords);
        // echo PHP_EOL;
        // print_r("data_lineage: " . $prettySourceLog);
    } else {
        echo PHP_EOL;
        // print_r("No Snowflake Records affected - " . ($last_update_formatted ?? ''));
        print_r("No Snowflake Records affected" . (isset($last_update) ? "for - " . $last_update : ''));
    }
}

print_r("\n");
print_r("Total timelapse in seconds: " . $timediff . " s");
