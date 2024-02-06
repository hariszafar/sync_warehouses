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
// C:/scripts/sync/ETL.php --days_old=1000 --tables=invoice_items --target=RDS,SNOWFLAKE;

function extractHCPCS($paragraph)
{
    $pattern = '/\b[A-Z]{1}[0-9]{4}\b/'; // Pattern to match HCPCS codes
    preg_match_all($pattern, $paragraph, $matches); // Perform regex matching

    $hcpcsArray = $matches[0]; // Extract matched HCPCS codes
    $hcpcsString = implode(',', $hcpcsArray); // Join codes with commas

    return $hcpcsString; // Return HCPCS codes separated by commas
}


$targetedExecution = false; // flag to check whether a specific script has been targeted
$snowflakeTargeted = false; // flag to check whether snowflake has been targeted
$rdsTargeted = false; //flag to check whether rds has been targeted
$targets = [];
if (defined('PHP_SAPI') && 'cli' === PHP_SAPI) {
    $GLOBALS['_SESSION'] = [];
    // get options eg ETL.php --type therapist
    $options = getopt(null ?? '', ['tables:', 'days_old:', 'logging_off:', 'target:', 'verbose:', 'ignore:', 'debug_logging:', 'local_testing:']); // returns an array
    //var_dump($options);
    //  exit;
} elseif (session_status() == PHP_SESSION_NONE) { // uses session to store data api token
    session_start();
    header("Access-Control-Allow-Origin: *");
}
$localTesting = (isset($options['local_testing']) || isset($_REQUEST['local_testing'])) ?? false;

require_once(__DIR__ . DIRECTORY_SEPARATOR . 'config.php');
if (!$localTesting) {
    require_once(__DIR__ . DIRECTORY_SEPARATOR . 'FM_extract.php');
} else {
    require_once(__DIR__ . DIRECTORY_SEPARATOR . 'lib/mock/FM_extract.php'); //This is actually a fallback to a local MySQL database
}
require_once(__DIR__ . DIRECTORY_SEPARATOR . 'RDS_load.php');

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
    $targetedExecution = true;
    $rdsTargeted = true;
}

$verboseLogging = (isset($options['verbose']) || isset($_REQUEST['verbose'])) ?? false;
if ($verboseLogging) {
    $verboseLogLevel = ((int) ($options['verbose'] ?? $_REQUEST['verbose']) > 0) ? (int) ($options['verbose'] ?? $_REQUEST['verbose']) : 1;
} else {
    $verboseLogLevel = 1;
}
//require_once(__DIR__ . DIRECTORY_SEPARATOR . '/lib/CloudWatchLogger.php');

// if (stripos(strtolower($options['target']), 'snowflake') > -1) {
//   include 'snowflake_demo.php';
// }

// optional parameters maybe passed in;
// $tables = an array of table names;
// $start_date = date from which all modified records should be extracted

// ************************************************
function sumRes($ur)
{
    $res_sum = [0, 0, 0];
    foreach ($ur as $r) {
        $res_sum[0] += $r[0];
        $res_sum[1] += $r[1];
        $res_sum[2] += $r[2];
    }
    return $res_sum;
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
$dest_table_list = (!empty($tablesParam)) ? $tablesParam : [
    'activity',
    'associate_documents',
    'accesslog', //comments out 03-22-23
    "bills",
    "billitems",
    "contact_methods",
    "company_request_types",
    "deposits",
    "expenses",
    "documents",
    "zipcodes",
    'invoice_activity',
    'invoices',
    'invoice_items',
    'lines_of_business',
    'locations',
    'managed_companies',
    'member',
    'narrative_report',
    'outcomes',
    'payer',
    'payments',
    'request',
    'request_fees',
    'request_subrequests',
    'reviewer',
    'therapist',
    'therapist_networks',
    'valuelist',
    'users'
];

//map primary keys with the respective tables
$primaryKeyPairs = [
    'activity' => 'id',
    'associate_documents' => 'id',
    'accesslog' => 'recordId', //comments out 03-22-23
    "bills" => 'id',
    "billitems" => 'id',
    "contact_methods" => 'id',
    "company_request_types" => 'id',
    "deposits" => 'id',
    "expenses" => 'id',
    "etl_log" => 'id',
    "documents" => 'id',
    "zipcodes" => 'id',
    'invoice_activity' => 'recordId',
    'invoices' => 'id',
    'invoice_items' => 'id',
    'lines_of_business' => 'id',
    'locations' => 'id',
    'managed_companies' => 'id',
    'member' => 'recordId',
    'narrative_report' => 'id',
    'outcomes' => 'id',
    'payer' => 'id',
    'payments' => 'recordId',
    'request' => 'id',
    'request_fees' => 'id',
    'request_subrequests' => 'id',
    'reviewer' => 'recordId',
    'therapist' => 'recordId',
    'therapist_networks' => 'id',
    'therapy_networks' => 'id', //added
    'valuelist' => 'recordId',
    'users' => 'recordId'
];

if (!defined('DATETIME_SEARCH_PLACEHOLDER')) {
    define("DATETIME_SEARCH_PLACEHOLDER", "DATETIME_SEARCH");
}
// Map the source table names (keys) to the search query (values)
$sourceSearchQuery = [
    'accesslog' => [
        ['timestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'associate_documents' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'documents' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER],
        ['filename' => "=", "omit" => 'true']
       ],
    'deposits' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'expenses' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'bills' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'billitems' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'contact_methods' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'valuelist' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'zipcodes' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'narrative_report' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'therapist' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER, 'type' => '=therapist'],
        ['nameFull' => "=", 'omit' => 'true']
    ],
    'users' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER, 'type' => '=user'],
        ['nameFull' => "=", 'omit' => 'true']
    ],
    'reviewer' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER, 'type' => '=reviewer'],
        ['nameFull' => "=", 'omit' => 'true']
    ],
    'therapy_networks' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'payer' => [
        ['category' => '=Payer']
    ],
    'member' => [
        ['type' => '=Patient']
    ],
    'request' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER],
        //['id' => '> 0'],
        ['payer_name' => 'Test', 'omit' => 'true']
    ],
    'request_subrequests' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER],
        ['Requests::payer_name' => 'Test', 'omit' => 'true'],
        ['additional_consideration' => "1", 'omit' => 'true']
    ],
    'request_fees' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'invoices' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'invoice_items' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'activity' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'locations' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'payments' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'invoice_activity' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'outcomes' => [
        ['id' => "> 0"]
    ],
    'managed_companies' => [
        ['id' => "> 0"]
    ],
    'lines_of_business' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
];

// valid search timestamp column names
$validTimestampColumns = [
    'modificationHostTimestamp',
    'timestamp'
];


// Map the destination table names (keys) to the source table names (values)
$tablesMap = [
    'accesslog' => 'accesslog',
    'associate_documents' => 'licenses',
    'documents' => 'documents',
    'deposits' => 'deposits',
    'expenses' => 'expenses',
    'bills' => 'bills',
    'billitems' => 'billitems',
    'contact_methods' => 'contact_methods',
    'valuelist' => 'valuelist',
    'zipcodes' => 'zipcodes',
    'narrative_report' => 'narrative_report',
    'therapist' => 'therapist',
    'users' => 'users',
    'reviewer' => 'reviewer',
    'therapy_networks' => 'therapy_networks',
    'payer' => 'payer',
    'member' => 'member',
    'request' => 'request',
    'request_subrequests' => 'request_subrequests',
    'request_fees' => 'request_fees',
    'invoices' => 'invoices',
    'invoice_items' => 'invoice_items',
    'activity' => 'activity',
    'locations' => 'locations',
    'payments' => 'payments',
    'invoice_activity' => 'invoice_activity',
    'outcomes' => 'outcomes',
    'managed_companies' => 'managed_companies',
    'lines_of_business' => 'lines_of_business',
];

$tableColumnMaps = [
    'accesslog' => [
        "timestamp" => "date_created"
    ],
    'associate_documents' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "license_type" => "type",
        "license_number" => "documentnumber",
        "id_Entity" => "associateid",
        "entity_email" => "associateemail",
        "date_exp" => "date_expired"
    ],
    'documents' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "creationAccountName" => "created_by",
        "modificationAccountName" => "modified_by"
    ],
    'deposits' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'expenses' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'bills' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'billitems' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'contact_methods' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "Entities::type" => "contact_type",
        "isPrimary" => "is_primary"
    ],
    'valuelist' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'zipcodes' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'narrative_report' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "id_request" => "request_id"
    ],
    'therapist' =>  [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'users' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'reviewer' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'therapy_networks' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'payer' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'member' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'request' => [
        "Entities Requests Reviewer Lookup::nameFull" => "reviewer",
        "Entities Requests Therapist::nameFull" => "therapist",
        "Entities Requests Therapist::primary_street_1" => "therapist_street",
        "Entities Requests Therapist::primary_state" => "therapist_state",
        "Entities Requests Therapist::primary_city" => "therapist_city",
        "Entities Requests Therapist::primary_zip" => "therapist_zip",
        "Entities Requests Nurse::nameFull" => "nurse",
        "payer_type" => "line_of_business",
        "Invoices::date_paid" => "date_paid",
        "payer_name" => "plan",
        "member_state" => "state",
        "creationHostTimestamp" => "date_created",
        "modificationHostTimestamp" => "date_modified",
        "creationAccountName" => "created_by",
        "long" => "lng",
        "amount_calc" => "amount"
    ],
    'request_subrequests' => [
        "modificationHostTimestamp" => "date_modified",
        "Requests::payer_name" => "payer_name",
        "Requests::assessment_id" => "assessment_id"
    ],
    'request_fees' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "date" => "date_fee"
    ],
    'invoices' => [
        "balancePaymentsUnapplied" => "payments_unapplied",
        "Entities Invoices Payer::nameFull" => "plan",
        "Entities Invoices Payer::billing_payment_due" => "billing_payment_due",
        "modificationHostTimestamp" => "date_modified",
        "costSubtotal" => "cost_subtotal",
        "invoiceNumber" => "invoice_number",
        "billing_period_start" => "date_billing_start"
    ],
    'invoice_items' => [
        "modificationHostTimestamp" => "date_modified",
        "id_Request" => "id_request",
        "id_Invoice" => "id_invoice"
    ],
    'activity' => [
        "modificationHostTimestamp" => "date_modified"
    ],
    'locations' => [
        "modificationHostTimestamp" => "date_modified",
        "long" => "lng"
    ],
    'payments' => [
        "modificationHostTimestamp" => "date_modified",
        "modificationAccountName" => "modified_by"
    ],
    'invoice_activity' => [
        "modificationHostTimestamp" => "date_modified"
    ],
    'outcomes' => [
        "modificationHostTimestamp" => "date_modified"
    ],
    'managed_companies' => [
        "modificationHostTimestamp" => "date_modified"
    ],
    'lines_of_business' => [
        "modificationHostTimestamp" => "date_modified"
    ],

];


$source_list = [];

// ************  the follwoing line should be commented before being deployed to production **************************
//$start_date ="01/01/2010";

// default first date time in case the lastSyncedTimestamp can't be retrieved from either the sync_status table or the source table
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


//var_dump($options);
//exit;

//print_r($dest_table_list);
//print_r($days_old);
// exit;

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

//  $last_update = (isset($start_date))? $start_date  : $yesterdays; // defaults to yesterday;
$limit = 1000;

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
        // Fetch the last synced timestamp for the current table from the sync_status table
        // $lastSyncedTimestamp = $snow->getLastSyncedTimestamp($dest_table);
    } catch (\Throwable $th) {
        die("Error: Unable to establish connection with Snowflake. [" . $th->getCode()
            . "]: " . $th->getMessage());
    }
}

//$start_date ="01/01/2010";
//  $days_old = isset($options['days_old'])? $options['days_old'] : 5;

// $yesterdays = date("m/d/Y H:i:s", strtotime('-' . $days_old . ' days'));

$log = [];
$total_records = 0;
$timeSort = ["fieldName" => "modificationHostTimestamp", "sortOrder" => "ascend"];
$starttime = microtime(true);
$rdsTimeLapse = [];
$snowflakeTimeLapse = [];
//$log[] = "Sync records modified date ".$last_update;
//$log[] = "-------------------------------------------------------";


/**
 * Prepare the search criteria for extracting data.
 *
 * @param Loader $targetLoader The target destination.
 * @param bool $daysOldSet Flag to check if days offset is set.
 * @param int $daysOffset The number of days offset.
 * @param string|null $lastUupdate The last update timestamp.
 * @param string $destTable The destination table.
 * @param array $sourceSearchQuery The source search query.
 * 
 * @return array|null The prepared search criteria.
 */
function prepareSearch(Loader &$targetLoader, $daysOldSet = false, $daysOffset = 0, $lastUpdate = null, $destTable = '', $sourceSearchQuery = [])
{
    global $defaultFirstDateTime, $localTesting, $verboseLogLevel;
    $search = $sourceSearchQuery ?? [];
    $isTimestampTable = false;
    if (!$targetLoader) {
        throw new Exception("Target destination specific loader not set.");
    }
    
    if (!empty($search)) {
        // check if this is a table which contain a timestamp column
        $isTimestampTable = searchContainsTimestamps($search);
        
        // if this is a table which doesn't contain a timestamp column, return the search as is
        if (!$isTimestampTable) {
            return $search;
        } else {
            // If the days_old parameter is not set, fetch & use the lastSyncedTimestamp
            if (!$daysOldSet) {
                // $lastSyncedTimestamp = $targetLoader->getLastSyncedTimestamp($destTable);
                $lastSyncedTimestamp = $targetLoader->getLastSyncedTimestamp($destTable) ?? $defaultFirstDateTime;
                // if the timestamp contains trailing zeros (e.g .0000...) remove them
                // $lastSyncedTimestamp = rtrim($lastSyncedTimestamp, '.0'); //rtrim was removing the zeros in the second after the colon in the timestamp

                $lastSyncedTimestamp = explode('.', $lastSyncedTimestamp ?? '')[0]; //explode the timestamp and remove the trailing zeros
                //This section will now be skipped as we're falling back to the default first datetime
                // echo "initial \$lastSyncedTimestamp = {$lastSyncedTimestamp}" . PHP_EOL;
                // if the lastSyncedTimestamp is not set, then we'll fetch the very first lastmodified timestamp from the source table from FM_extract
                // if (!$lastSyncedTimestamp) {
                //     if($verboseLogLevel > 1) {
                //         echo "Couldn't Fetch datetime from sync_table." . PHP_EOL;
                //     }
                //     $lastSyncedTimestamp = getTableFirstLastModifiedTimestamp($destTable); //FM format should be 'm/d/Y H:i:s'
                //     if($verboseLogLevel > 1) {
                //         echo "Getting first timestamp from FM source table = {$lastSyncedTimestamp}" . PHP_EOL;
                //     }
                //     if (!$lastSyncedTimestamp) {
                //         if($verboseLogLevel > 1) {
                //             echo "Couldn't Fetch datetime from FM source table either. Setting lastSyncedTimestamp to default ({$defaultFirstDateTime})" . PHP_EOL;
                //         }
                //         // if the lastSyncedTimestamp is not set, then we'll use the default first datetime
                //         $lastSyncedTimestamp = $defaultFirstDateTime; //we've set the format to 'm/d/Y H:i:s' above
                //     }
                // } else {
                //     // We've retrieved the last synced timesatamp from either RDS or Snowflake, and the expected format is 'm/d/Y H:i:s'
                //     // Now, let's convert it to 'm/d/Y H:i:s'
                //     if($verboseLogLevel > 1) {
                //         echo "Retrieved lastSyncedTimestamp from sync_table = {$lastSyncedTimestamp}" . PHP_EOL;
                //     }

                //     $timestamp = date_create_from_format('m/d/Y H:i:s', $lastSyncedTimestamp);
                //     $lastSyncedTimestamp = date_format($timestamp, 'm/d/Y H:i:s');

                //     if($verboseLogLevel > 1) {
                //         echo "Converted lastSyncedTimestamp to FM format = {$lastSyncedTimestamp}" . PHP_EOL;
                //     }
                // }

                // // echo "after setting timestamp \$lastSyncedTimestamp = {$lastSyncedTimestamp}" . PHP_EOL;
                // // $offsetDatetime = date("m/d/Y H:i:s", (strtotime($lastSyncedTimestamp) + ($daysOffset * 24 * 60 * 60)));
                // $currentDateTime = date('m/d/Y H:i:s');

                // $lastSyncedDateTime = date_create_from_format('m/d/Y H:i:s', $lastSyncedTimestamp);
                // if ($verboseLogLevel > 1) {
                //     echo "lastSyncedDateTime => " . var_dump($lastSyncedDateTime) . PHP_EOL;
                // }
                // $offsetDateTime = date_create_from_format('m/d/Y H:i:s', $lastSyncedTimestamp);
                // if ($verboseLogLevel > 1) {
                //     echo "offsetDateTime (before date_add) => " . var_dump($offsetDateTime) . PHP_EOL;
                // }
                // $offsetDateTime  = date_add($offsetDateTime, date_interval_create_from_date_string($daysOffset . ' days'));
                // if ($verboseLogLevel > 1) {
                //     echo "offsetDateTime (after date_add) => " . var_dump($offsetDateTime) . PHP_EOL;
                // }

                // if ($offsetDateTime > date_create_from_format('m/d/Y H:i:s', $currentDateTime)) {
                //     $offsetDateTime = date_create_from_format('m/d/Y H:i:s', $currentDateTime);
                // }
                // if ($verboseLogLevel > 1) {
                //     echo "offsetDateTime (after comparison with current date) => " . var_dump($offsetDateTime) . PHP_EOL;
                // }

                // // loaders accept the datetime in 'Y-m-d H:i:s' (which is close to ISO 8601 format, excluding the \T for timezone)
                // // TODO: get rid of setting the offsetDatetime here. It should be done during the updateTable method call
                // $targetLoader->setOffsetDatetime(date_format($offsetDateTime, 'Y-m-d H:i:s'));
                // if ($localTesting) {
                //     $lastSyncedTimestamp = date_format($lastSyncedDateTime, 'Y-m-d H:i:s');
                //     $offsetTimestamp = date_format($offsetDateTime, 'Y-m-d H:i:s');
                // } else {
                //     $lastSyncedTimestamp = date_format($lastSyncedDateTime, 'm/d/Y H:i:s');
                //     $offsetTimestamp = date_format($offsetDateTime, 'm/d/Y H:i:s');
                // }

                if ($verboseLogLevel > 1) {
                    echo "Timestamps for search values: " . PHP_EOL;
                    echo "lastSyncedTimestamp => " . var_dump($lastSyncedTimestamp) . PHP_EOL;
                    // echo "offsetTimestamp => " . var_dump($offsetTimestamp) . PHP_EOL;
                    echo "Serach before replacing:" . PHP_EOL;
                    var_dump($search);
                }
                foreach ($search as $key => $value) {
                    // $search[$key] = str_replace(DATETIME_SEARCH_PLACEHOLDER, $lastSyncedTimestamp . '...' . $offsetTimestamp, $value);
                    // We're not searching between timestamps anymore
                    $search[$key] = str_replace(DATETIME_SEARCH_PLACEHOLDER, ">=" . $lastSyncedTimestamp, $value);
                }
                if ($verboseLogLevel > 1) {
                    echo "Serach after replacing:" . PHP_EOL;
                    var_dump($search);
                }
            } else {
                foreach ($search as $key => $value) {
                    $search[$key] = str_replace(DATETIME_SEARCH_PLACEHOLDER, ">=" . $lastUpdate, $value);    
                }
            }
        }

    }

    return $search;
}

/**
 * Check if table search array contains timestamp columns.
 * 
 * @param array $search The search criteria.
 * @return bool Whether the table is a timestamp table.
 */
function searchContainsTimestamps($search = [])
{
    $timestampTable = false;
    if (!empty($search)) {
        // check if this is a table which doesn't contain a timestamp column
        foreach ($search as $key => $value) {
            if (is_array($value)) {
                $timestampTable = searchContainsTimestamps($value);
                if ($timestampTable) {
                    break;
                }
            } else {
                if (strpos($value, DATETIME_SEARCH_PLACEHOLDER) !== false) {
                    $timestampTable = true;
                    break;
                }
            }
        }
    }

    return $timestampTable;
}

/**
 * Check if data extraction should be performed only once.
 *
 * @param mixed $rdsSearch The search criteria for RDS.
 * @param mixed $snowflakeSearch The search criteria for Snowflake.
 * @return bool Whether data extraction should be performed only once.
 */
function shouldExtractOnce($rdsSearch = [], $snowflakeSearch = [])
{
    $extractOnce = false;

    if (isset($rdsSearch) && isset($snowflakeSearch) && $rdsSearch == $snowflakeSearch) {
        $extractOnce = true;
    } else if (!isset($rdsSearch) || !isset($snowflakeSearch)) {
        $extractOnce = true;
    }

    return $extractOnce;
}

// TODO: This function is not used. Keep commented out for now.
/**
 * Retrieves the first 'last modified' timestamp of a specified destination table.
 * This is used as the offset datetime for the next sync iteration. The returned timestamp is in the format m/d/Y H:i:s.
 *
 * @param string $destTable The name of the destination table.
 * @return string|null The first modified timestamp of the destination table, or null if no records are found.
 */
// function getTableFirstLastModifiedTimestamp($destTable)
// {
//     global $db, $host, $tablesMap, $localTesting, $verboseLogLevel;
//     $firstLastModifiedTimestamp = null;
//     $sort = getTimeSort($destTable);
//     if ($verboseLogLevel > 1) {
//         echo "getTableFirstLastModifiedTimestamp() => \$sort = " . var_dump($sort) . PHP_EOL;
//         echo PHP_EOL . "ABOUT TO EXECUTE". PHP_EOL;
//     }

//     // $fquery = new FM_extract($db, $host, $tablesMap[$destTable], $destTable, [], 1, 0, ["fieldName" => $searchColumn, "sortOrder" => "ascend"], [], true);
//     // $fquery = new FM_extract($db, $host, $tablesMap[$destTable], $destTable, [], 1, 0, $sort, [], true); // extra debug parameter for mock testing
//     // $fquery = new FM_extract($db, $host, $tablesMap[$destTable], $destTable, [['modificationHostTimestamp' => '>'.'01/01/1970 00:00:00']], 1, 0, $sort, []);
//     // $fquery = new FM_extract($db, $host, $tablesMap[$destTable], $destTable, [['modificationHostTimestamp' => '*']], 1, 0, null, []);

//     $fquery = new FM_extract($db, $host, $tablesMap[$destTable], $destTable, [['modificationHostTimestamp' => '*']], 1, 0, $sort, []);
//     $rs = $fquery->getRecordSet();
//     // $rs = $fquery->dbse->find([], ['modificationHostTimestamp', 'Ascend'], 1, 0);
//     // $rs = $fquery->dbse->all();


//     if ($verboseLogLevel > 1) {
//         echo "getTableFirstLastModifiedTimestamp() => \$rs = " . var_dump($rs) . PHP_EOL;
//     }
//     $rs_array = json_decode($rs ?? '', true);
//     if (is_array($rs_array) && count($rs_array) > 0) {
//         // $firstLastModifiedTimestamp = $rs_array[0][$sort["fieldName"]];
//         $firstLastModifiedTimestamp = $rs_array[0][$sort[0]["fieldName"]];
//     }
//     if ($localTesting) {
//         // $firstLastModifiedTimestamp = date('Y/m/d H:i:s', strtotime($firstLastModifiedTimestamp));
//         if( strpos($firstLastModifiedTimestamp, '/') !== false) {
//             $date = date('Y/m/d H:i:s', $firstLastModifiedTimestamp);
//         } else {
//             //otherwise, consider the date separator to contain dashes
//             $date = date_create_from_format("Y-m-d H:i:s", $firstLastModifiedTimestamp);
//         }
//     } else {
//         if( strpos($firstLastModifiedTimestamp, '/') !== false) {
//             $date = date_create_from_format("m/d/Y H:i:s", $firstLastModifiedTimestamp);
//         } else {
//             //otherwise, consider the date separator to contain dashes
//             $date = date_create_from_format("m-d-Y H:i:s", $firstLastModifiedTimestamp);
//         }
//     }
//     // update the $date variable, and take date one day back
//     date_sub($date, date_interval_create_from_date_string('1 day'));

//     $firstLastModifiedTimestamp = date_format($date, "m/d/Y 00:00:00");

//     return $firstLastModifiedTimestamp;
// }

/**
 * Returns the timestamp column used for sorting in the given destination table, with default ascending sort order.
 *
 * @param string $destTable The name of the destination table.
 * @return string The timestamp column used for sorting.
 */
function getTimeSort($destTable)
{
    global $sourceSearchQuery, $validTimestampColumns;
    $searchParams = $sourceSearchQuery[$destTable] ?? [];
    $searchColumn = $validTimestampColumns[0];
    foreach ($searchParams as $key => $searchArray) {
        foreach($searchArray as $searchKey => $value) {
            foreach ($validTimestampColumns as $timestampColumn) {
                if (strpos($searchKey, $timestampColumn) !== false) {
                    $searchColumn = $timestampColumn;
                    break 3;
                }
            }
        }
    }
    // return $searchColumn;
    $sort = [];
    $sort[] = ["fieldName" => $searchColumn, "sortOrder" => "ascend"];
    return $sort;
}

/**
 * Updates the ETL log in the database with the summary of records modified, added, and unchanged.
 *
 * @param object $rds The RDS object used for database operations.
 * @param array $log The log array containing the data table and the summary of update results.
 * @param int $timeLapse The time lapse value for the data table.
 * @param string $lastUpdate The last update timestamp.
 * @return void
 */
function rdsEtlLogUpdate(&$rds, $log, $timeLapse, $lastUpdate)
{
    global $primaryKeyPairs, $logging;

    $nochange_records = 0;
    $change_records = 0;
    $add_records  = 0;
    $dataTable = $log[0];
    $updateResultSum = $log[1];

    $records = [];
    $record = [];
    $record["data_table"] = $dataTable;
    $record["records_modified"] =  $updateResultSum[2] ?? 0;
    $record["records_added"] =  $updateResultSum[1] ?? 0;
    $record["records_unchanged"] =  $updateResultSum[0] ?? 0;
    $record["date_modified"] = $lastUpdate;
    $record["date_time"] = date("Y-m-d H:i:s");
    $record["timelapse"] = $timeLapse ?? 0;
    $nochange_records += $updateResultSum[0] ?? 0;
    $change_records += $updateResultSum[2] ?? 0;
    $add_records  += $updateResultSum[1] ?? 0;
    $records[] = $record;

    $total_records = ($nochange_records + $change_records + $add_records);

    if ($total_records > 0) {
        $json_log = json_encode($records, true);
        if ($logging) {
            $dest_table = "etl_log";
            $primaryKey = $primaryKeyPairs[$dest_table];
            $rds->updateTable($json_log, $dest_table, $primaryKey);
        }
    }
}

function snowEtlLogUpdate($snow, $logRecord, $timeLapse, $lastModifiedTimestamp)
{
    global $primaryKeyPairs, $logging;
    $snowflakeRecords = [];
    foreach ($logRecord as $table => $affectedRows) {
        // $update_date = date_create($last_update ?? $snowLastModified[$table]);
        // $update_date = isset($laste_update) ? date_create_from_format("m/d/Y H:i:s", $last_update) : date_create_from_format("Y-m-d H:i:s", $snowLastModified[$table]);
        // $last_update_formatted =   date_format($update_date, 'Y-m-d');

        $snowflakeRecord = [];
        $snowflakeRecord["data_table"] = $table;
        /* Unlike MySQL, even in the case of an update
        where nothing is changed, snowflake returns it as an affected Row
        So, we can't track unchanged records separately.
        Therefore, we're only recording the affected rows returned by snowflake.
        */
        $snowflakeRecord["affected_rows"] =  $affectedRows;
        $snowflakeRecord["date_modified"] = $lastModifiedTimestamp;
        $snowflakeRecord["date_time"] = date("Y-m-d H:i:s");
        // $snowflakeRecord["timelapse"] = (int)array_sum($snowflakeTimeLapse); //this is actually the total timelapse of the script, not just snowflake
        $snowflakeRecord["timelapse"] = (int)$timeLapse; //this is the total timelapse for the particular table (including multiple iterations)
        $snowflakeRecords[] = $snowflakeRecord;
    }

    if ($logging) {
        $json_log = json_encode($snowflakeRecords, true);
        $dest_table = "etl_log";
        $primaryKey = $primaryKeyPairs[$dest_table];
        $snow->updateTable($json_log, $dest_table, $primaryKey, true);
    }
}

// Let's sync the data for each table (or each requested/targeted table)
foreach ($tablesMap as $dest_table => $source_table)
{
    if (in_array($dest_table, $dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
        $search = $rdsSearch = $snowflakeSearch = null;
        $isTimestampTable = searchContainsTimestamps($sourceSearchQuery[$dest_table] ?? []);
        // $sort = ($isTimestampTable) ? $timeSort : null;
        $sort = ($isTimestampTable) ? getTimeSort($dest_table) : null;
        // extract timestamp column name in a variable for later use if this is a timestamp table
        if ($isTimestampTable) {
            $timestampColumn = $sort[0]["fieldName"];
            $mappedTimestampColumn = $column_map[$timestampColumn] ?? $timestampColumn;
        }
        if (!$targetedExecution || $rdsTargeted) {
            // $rdsSearch = ($isTimestampTable) ? $search : prepareSearch($rds, $daysOldSet, $daysOffset, $last_update, $dest_table, $sourceSearchQuery[$source_table] ?? null);
            $rdsSearch = prepareSearch($rds, $daysOldSet, $daysOffset, $last_update ?? null, $dest_table, $sourceSearchQuery[$dest_table] ?? null);
        }
        if (!$targetedExecution || $snowflakeTargeted) {
            // $snowflakeSearch = ($isTimestampTable) ? $search : prepareSearch($snow, $daysOldSet, $daysOffset, $last_update, $dest_table, $sourceSearchQuery[$source_table] ?? null);
            $snowflakeSearch = prepareSearch($snow, $daysOldSet, $daysOffset, $last_update ?? null, $dest_table, $sourceSearchQuery[$dest_table] ?? null);
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
                        $rdsLastModifiedTimestamp[$dest_table] = date_format(
                            date_create_from_format('Y-m-d H:i:s', $rs_array[count($rs_array) - 1][$mappedTimestampColumn]),
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
                        $snowLastModifiedTimestamp[$dest_table] = date_format(
                            date_create_from_format('Y-m-d H:i:s', $rs_array[count($rs_array) - 1][$mappedTimestampColumn]),
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
                            $rdsLastModifiedTimestamp[$dest_table] = date_format(
                                date_create_from_format('Y-m-d H:i:s', $rs_array[count($rs_array) - 1][$mappedTimestampColumn]),
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
                            $snowLastModifiedTimestamp[$dest_table] = date_format(
                                date_create_from_format('Y-m-d H:i:s', $rs_array[count($rs_array) - 1][$mappedTimestampColumn]),
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
