<?php
/********************************************
Modified script by Ben Marchbanks ben@dme-cg.com or ben@alqemy
 Phone: 540 760 4104

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

function extractHCPCS($paragraph) {
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
    $options = getopt(null, ['tables:','days_old:','logging_off:', 'target:', 'verbose:', 'ignore:', 'debug_logging:']); // returns an array
  //var_dump($options);
  //  exit;
}
elseif (session_status() == PHP_SESSION_NONE) { // uses session to store data api token
    session_start();
    header("Access-Control-Allow-Origin: *");
}

require_once(__DIR__ . DIRECTORY_SEPARATOR . 'config.php');
require_once(__DIR__ . DIRECTORY_SEPARATOR . 'FM_extract.php');
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
//require_once(__DIR__ . DIRECTORY_SEPARATOR . '/lib/CloudWatchLogger.php');

// if (stripos(strtolower($options['target']), 'snowflake') > -1) {
//   include 'snowflake_demo.php';
// }

// optional parameters maybe passed in;
// $tables = an array of table names;
// $start_date = date from which all modified records should be extracted

//************************************************
function sumRes($ur){
  $res_sum = [0,0,0];
  foreach($ur as $r){
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

     $source_list = [];

// ************  the follwoing line should be commented before being deployed to production **************************


     //$start_date ="01/01/2010";
     $days_old = isset($options['days_old'])? $options['days_old'] : 5;

     $yesterdays = date("m/d/Y H:i:s", strtotime( '-'.$days_old.' days' ) );


//var_dump($options);
//exit;

  //print_r($dest_table_list);
  //print_r($days_old);
  // exit;

     $logging =  isset($options['logging_off']) ? 0 : 1;
     $debug_logging = (isset($options['debug_logging']) && $options['debug_logging'] == 0) ? 0 : 1;
     $db = $config['fm_db']; // database name on FileMaker server
     $host = $config['fm_host']; // ip address of server

     $last_update = (isset($start_date))? $start_date  : $yesterdays; // defaults to yesterday;
     $limit = 1000;
      
      if (!$targetedExecution || $rdsTargeted) {
        $rds = new RDS_load($config);
        $rds->setVerboseExecutionLogging($verboseLogging);
        $rds->clearLogFile(); // initialize log file
        $rds->setDebugLogging($debug_logging);
      }
      if (!$targetedExecution || $snowflakeTargeted) {
        try {
          //code...
          $snow = new SnowflakeLoader($config['snowflake']);
          $snow->setVerboseExecutionLogging($verboseLogging);
          $snow->clearLogFile(); // initialize log file
          $snow->setDebugLogging($debug_logging);
        } catch (\Throwable $th) {
          die("Error: Unable to establish connection with Snowflake. [" . $th->getCode() 
          . "]: " . $th->getMessage() );
        }
      }
     $log = [];
     $total_records = 0;
     $sort = "modificationHostTimestamp";
     $starttime = microtime(true);
     $rdsTimeLapse = [];
     $snowflakeTimeLapse = [];
     //$log[] = "Sync records modified date ".$last_update;
     //$log[] = "-------------------------------------------------------";

/// sync the following mysql_tables;

//************ accesslog *****************

      // name of table in the mySQL database
         $dest_table = "accesslog";
         // name of the table in FileMaker
         $source_table = "accesslog";


         $search = [
          ['timestamp' => ">".$last_update]
         ];
         $offset = 0;
         $column_map=[  "timestamp" => "date_created"];

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);
           //var_dump($fquery);
           //exit;

           $update_res =  [];
           do{
               $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
               $rs_array = json_decode($rs);
               $primaryKey = $primaryKeyPairs[$dest_table]; // Provide Primary Key for table
               if (!$targetedExecution || $rdsTargeted) {
                 $begin = microtime(true);
                 $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                 $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                 $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
               }
               if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);

                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
               }
               $offset += $limit;
               $fquery->offset = $offset;
             }while (count((array)$rs_array) > 0 );


            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
             $log[] = [$dest_table,sumRes($update_res)];
         }

         //************ associate_documents *****************

         // name of table in the mySQL database
         $dest_table = "associate_documents";
         // name of the table in FileMaker
         $source_table = "licenses";


         $search = [
             ['modificationHostTimestamp' => ">".$last_update]
         ];
         $offset = 0;
         $column_map=[
             "modificationHostTimestamp" => "date_modified",
             "creationHostTimestamp" => "date_created",
             "license_type" => "type",
             "license_number" => "documentnumber",
             "id_Entity" => "associateid",
             "entity_email" => "associateemail",
             "date_exp" => "date_expired"
         ];

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
             $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

             $update_res =  [];
             do{
                 $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL

                 $rs_array = json_decode($rs);
                 $primaryKey = $primaryKeyPairs[$dest_table]; // Primary Key for table
                 if (!$targetedExecution || $rdsTargeted) {
                    $begin = microtime(true);
                    $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                    $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                    $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                 }
                 if (!$targetedExecution || $snowflakeTargeted) {
                  $begin = microtime(true);
                  $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                  $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                  
                  $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                  $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                  
                 }
                 $offset += $limit;
                 $fquery->offset = $offset;
             }while (count((array)$rs_array) > 0 );


             $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
             $log[] = [$dest_table,sumRes($update_res)];
         }


         //************ documents *****************

      // name of table in the mySQL database
         $dest_table = "documents";
         // name of the table in FileMaker
         $source_table = "documents";


         $search = [
          ['modificationHostTimestamp' => ">".$last_update],
          ['filename' => "=" , "omit" =>'true']
         ];
         $offset = 0;
         $column_map=[
           "modificationHostTimestamp" => "date_modified",
            "creationHostTimestamp" => "date_created",
            "creationAccountName" => "created_by",
            "modificationAccountName" => "modified_by"
         ];

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

           $update_res =  [];
           do{
               $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
               $rs_array = json_decode($rs);
               $primaryKey = $primaryKeyPairs[$dest_table];
               if (!$targetedExecution || $rdsTargeted) {
                  $begin = microtime(true);
                  $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                  $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                 $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
               }
               if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                
                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                
               }
               $offset += $limit;
               $fquery->offset = $offset;
             }while (count((array)$rs_array) > 0 );


            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
             $log[] = [$dest_table,sumRes($update_res)];
         }

//************ deposits *****************

      // name of table in the mySQL database
         $dest_table = "deposits";
         // name of the table in FileMaker
         $source_table = "deposits";


         $search = [
          ['modificationHostTimestamp' => ">".$last_update]
         ];
         $offset = 0;
         $column_map=[
           "modificationHostTimestamp" => "date_modified",
            "creationHostTimestamp" => "date_created"
         ];

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

           $update_res =  [];
           do{
               $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
               $rs_array = json_decode($rs);
               $primaryKey = $primaryKeyPairs[$dest_table];
               if (!$targetedExecution || $rdsTargeted) {
                $begin = microtime(true);
                $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              }
               if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                
                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                
               }
               $offset += $limit;
               $fquery->offset = $offset;
             }while (count((array)$rs_array) > 0 );


            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
             $log[] = [$dest_table,sumRes($update_res)];
         }


  //************ expenses *****************

               // name of table in the mySQL database
                  $dest_table = "expenses";
                  // name of the table in FileMaker
                  $source_table = "expenses";


                  $search = [
                   ['modificationHostTimestamp' => ">".$last_update]
                  ];
                  $offset = 0;
                  $column_map=[
                    "modificationHostTimestamp" => "date_modified",
                     "creationHostTimestamp" => "date_created"
                  ];

                  if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
                    $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

                    $update_res =  [];
                    do{
                        $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                        $rs_array = json_decode($rs);
                        $primaryKey = $primaryKeyPairs[$dest_table];
                        if (!$targetedExecution || $rdsTargeted) {
                          $begin = microtime(true);
                          $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                          $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                          $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                        }
                        if (!$targetedExecution || $snowflakeTargeted) {
                          $begin = microtime(true);
                          $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                          $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                          
                          $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                          $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                          
                        }
                        $offset += $limit;
                        $fquery->offset = $offset;
                      }while (count((array)$rs_array) > 0 );


                     $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
                      $log[] = [$dest_table,sumRes($update_res)];
                  }


//************ bills *****************

      // name of table in the mySQL database
         $dest_table = "bills";
         // name of the table in FileMaker
         $source_table = "bills";

         $search = [
          ['modificationHostTimestamp' => ">".$last_update]
         ];
         $offset = 0;
         $column_map=[
           "modificationHostTimestamp" => "date_modified",
            "creationHostTimestamp" => "date_created"
         ];

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

           $update_res =  [];
           do{
                $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                $rs_array = json_decode($rs);
                $primaryKey = $primaryKeyPairs[$dest_table];
                if (!$targetedExecution || $rdsTargeted) {
                  $begin = microtime(true);
                  $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                  $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                  $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                }
                if (!$targetedExecution || $snowflakeTargeted) {
                  $begin = microtime(true);
                  $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                  $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                  
                  $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                  $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                }
                $offset += $limit;
                $fquery->offset = $offset;
             }while (count((array)$rs_array) > 0 );


             $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
             $log[] = [$dest_table,sumRes($update_res)];
         }

         //************ billitems *****************
                   // name of table in the mySQL database
                  $dest_table = "billitems";
                  // name of table in the FileMaker database
                  $source_table = "billitems";

                  $search = [
                   ['modificationHostTimestamp' => ">".$last_update]
                 ];
                  $offset = 0;
                  $column_map=[
                    "modificationHostTimestamp" => "date_modified",
                     "creationHostTimestamp" => "date_created"
                  ];

                  if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
                    $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

                    $update_res =  [];
                    do{
                        $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                        $rs_array = json_decode($rs);
                        $primaryKey = $primaryKeyPairs[$dest_table];
                        if (!$targetedExecution || $rdsTargeted) {
                          $begin = microtime(true);
                          $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                          $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                          $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                        }
                        if (!$targetedExecution || $snowflakeTargeted) {
                          $begin = microtime(true);
                          $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                          $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                          
                          $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                          $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                          
                        }
                        $offset += $limit;
                        $fquery->offset = $offset;
                      }while (count((array)$rs_array) > 0 );


                      $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
                      $log[] = [$dest_table,sumRes($update_res)];
                  }


//************ contact_methods *****************

        // name of table in the mySQL database
         $dest_table = "contact_methods";
         // name of the table in FileMaker
         $source_table = "contactMethods";


         $search = [
          ['modificationHostTimestamp' => ">".$last_update],
          ['Entities::type' => "=Patient" , "omit" =>'true']
         ];
         $offset = 0;
         $column_map=[
           "modificationHostTimestamp" => "date_modified",
            "creationHostTimestamp" => "date_created",
           "Entities::type" => "contact_type",
           "isPrimary" => "is_primary"
         ];

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

           $update_res =  [];
           do{
                $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                $rs_array = json_decode($rs);
                $primaryKey = $primaryKeyPairs[$dest_table];
                if (!$targetedExecution || $rdsTargeted) {
                  $begin = microtime(true);
                  $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                  $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                  $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                }
                if (!$targetedExecution || $snowflakeTargeted) {
                  $begin = microtime(true);
                  $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                  $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                  
                  $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                  $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                  
                }
                $offset += $limit;
                $fquery->offset = $offset;
             }while (count((array)$rs_array) > 0 );


            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
             $log[] = [$dest_table,sumRes($update_res)];
         }


//************ valuelist *****************

    // name of table in the mySQL database
     $dest_table = "valuelist";
    // name of the table in FileMaker
     $source_table = "valuelist";

     $search = [
       ['modificationHostTimestamp' => ">".$last_update]
     ];
     $offset = 0;
     $column_map=[ // normalizing of column names for better readability
       "modificationHostTimestamp" => "date_modified",
       "creationHostTimestamp" => "date_created"
     ];

     if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
       $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

       $update_res =  [];
       do{
            $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
            $rs_array = json_decode($rs);
            $primaryKey = $primaryKeyPairs[$dest_table];
            if (!$targetedExecution || $rdsTargeted) {
              $begin = microtime(true);
              $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
              $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
              $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
            }
            if (!$targetedExecution || $snowflakeTargeted) {
              $begin = microtime(true);
              $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
              $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
              
              $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
              $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              
            }
            $offset += $limit;
            $fquery->offset = $offset;
         }while (count((array)$rs_array) > 0 );

         $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
         $log[] = [$dest_table,sumRes($update_res)];
     }



//************ counties *****************

    // name of table in the mySQL database
     $dest_table = "zipcodes";
     // name of the table in FileMaker
     $source_table = "zipcodes";

     $search = [
       ['modificationHostTimestamp' => ">".$last_update]
     ];
     $offset = 0;
     $column_map=[ // normalizing of column names for better readability
       "modificationHostTimestamp" => "date_modified",
       "creationHostTimestamp" => "date_created"
     ];

     if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
       $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

       $update_res =  [];
       do{
            $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
            $rs_array = json_decode($rs);
            $primaryKey = $primaryKeyPairs[$dest_table];
            if (!$targetedExecution || $rdsTargeted) {
              $begin = microtime(true);
              $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
              $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
              $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
            }
            if (!$targetedExecution || $snowflakeTargeted) {
              $begin = microtime(true);
              $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
              $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
              
              $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
              $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              
            }
            $offset += $limit;
            $fquery->offset = $offset;
         }while (count((array)$rs_array) > 0 );

         $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
         $log[] = [$dest_table,sumRes($update_res)];
     }

     //************ narrative report *****************

    // name of table in the mySQL database
     $dest_table = "narrative_report";
     // name of the table in FileMaker
     $source_table = "NarrativeReport";

     $search = [
         ['creationHostTimestamp' => ">".$last_update]
     ];
     $offset = 0;
     $column_map=[ // normalizing of column names for better readability
         "modificationHostTimestamp" => "date_modified",
         "creationHostTimestamp" => "date_created",
         "id_request" => "request_id"
     ];

     if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
         $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

         $update_res =  [];
         do{
            $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
            $nrs = [];

            $rs_array = json_decode($rs);

            if(is_array($rs_array) ){
              foreach ($rs_array as $nrec) {
                $nt = $nrec -> text_upper;
                $hcpcs = extractHCPCS($nt);
                $nrec->hcpcs = $hcpcs;
                $nrs[] = $nrec;
              }
              $nrs_json = json_encode($nrs,true);
            }


            $primaryKey = $primaryKeyPairs[$dest_table];
            if (!$targetedExecution || $rdsTargeted) {
              $begin = microtime(true);
              $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($nrs_json, $dest_table, $primaryKey) : [0,0,0];
              $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
              $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
            }
            if (!$targetedExecution || $snowflakeTargeted) {
              $begin = microtime(true);
              $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
              $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
              
              $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($nrs_json, $dest_table, $primaryKey) : 0;
              $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
            }
            $offset += $limit;
            $fquery->offset = $offset;
         }while (count((array)$rs_array) > 0 );

         $arr_count = isset($rs_array) ? count((array)$rs_array) : 0;

         $source_list[$source_table] = $dest_table.":".$arr_count ;
         $log[] = [$dest_table,sumRes($update_res)];
     }


//************ therapist *****************

    // name of table in the mySQL database
     $dest_table = "therapist";
     // name of the table in FileMaker
     $source_table = "therapist";

     $search = [
       ['modificationHostTimestamp' => ">".$last_update,'type' => '=therapist'],
       ['nameFull' => "=" , 'omit' => 'true']
     ];
     $offset = 0;
     $column_map=[ // normalizing of column names for better readability
       "modificationHostTimestamp" => "date_modified",
       "nameFull" => "name"
     ];

     if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
       $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

       $update_res =  [];
       do{
          $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
          $rs_array = json_decode($rs);
          $primaryKey = $primaryKeyPairs[$dest_table];
          if (!$targetedExecution || $rdsTargeted) {
            $begin = microtime(true);
            $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
            $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
            $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
          }
          if (!$targetedExecution || $snowflakeTargeted) {
            $begin = microtime(true);
            $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
            $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
            
            $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
            $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
            
          }
          $offset += $limit;
          $fquery->offset = $offset;
         }while (count((array)$rs_array) > 0 );

         $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
         $log[] = [$dest_table,sumRes($update_res)];
     }

     //************ therapist *****************


     //************ users *****************

         // name of table in the mySQL database
          $dest_table = "users";
          // name of the table in FileMaker
          $source_table = "users";

          $search = [
            ['modificationHostTimestamp' => ">".$last_update,'subtype' => '=user'],
            ['modificationHostTimestamp' => ">".$last_update,'subtype' => '=DME'],
            ['nameFull' => "=" , 'omit' => 'true']
          ];
          $offset = 0;
          $column_map=[ // normalizing of column names for better readability
            "modificationHostTimestamp" => "date_modified",
            "nameFull" => "name"
          ];

          if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
            $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

            $update_res =  [];
            do{
                $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                $rs_array = json_decode($rs);
                $primaryKey = $primaryKeyPairs[$dest_table];
                if (!$targetedExecution || $rdsTargeted) {
                  $begin = microtime(true);
                  $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                  $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                  $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                }
                if (!$targetedExecution || $snowflakeTargeted) {
                  $begin = microtime(true);
                  $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                  $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                  
                  $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                  $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                  
                }
                $offset += $limit;
                $fquery->offset = $offset;
              }while (count((array)$rs_array) > 0 );

              $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
              $log[] = [$dest_table,sumRes($update_res)];
          }

          //************ users *****************



         // name of table in the mySQL database
          $dest_table = "reviewer";
          // name of the table in FileMaker
          $source_table = "reviewer";

          $search = [
            ['modificationHostTimestamp' => ">".$last_update,'reviewer' => '=1'],
            ['nameFull' => "=" , 'omit' => 'true']
          ];
          $offset = 0;
          $column_map=[ // normalizing of column names for better readability
            "modificationHostTimestamp" => "date_modified",
            "nameFull" => "name"
          ];

          if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
            $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

            $update_res =  [];
            do{
              $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
              $rs_array = json_decode($rs);
$primaryKey = $primaryKeyPairs[$dest_table];
              if (!$targetedExecution || $rdsTargeted) {
                $begin = microtime(true);
                $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              }
                            if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                
                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                
              }
              $offset += $limit;
              $fquery->offset = $offset;
            }while (count((array)$rs_array) > 0 );

            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
            $log[] = [$dest_table,sumRes($update_res)];
          }

          //************ reviewer *****************

         // name of table in the mySQL database
          $dest_table = "therapy_networks";
          // name of the table in FileMaker
          $source_table = "entities";

          $search = [
            ['modificationHostTimestamp' => ">".$last_update,'category' => '=Therapy Network'],
            ['nameFull' => "=" , 'omit' => 'true']
          ];
          $offset = 0;
          $column_map=[ // normalizing of column names for better readability
            "modificationHostTimestamp" => "date_modified",
            "nameFull" => "name"
          ];

          if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
            $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

            $update_res =  [];
            do{
                $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                $rs_array = json_decode($rs);
                $primaryKey = $primaryKeyPairs[$dest_table];
                if (!$targetedExecution || $rdsTargeted) {
                  $begin = microtime(true);
                  $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                  $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                  $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                }
                if (!$targetedExecution || $snowflakeTargeted) {
                  $begin = microtime(true);
                  $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                  $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                  
                  $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                  $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                  
                }
                $offset += $limit;
                $fquery->offset = $offset;
              }while (count((array)$rs_array) > 0 );

              $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
              $log[] = [$dest_table,sumRes($update_res)];
          }

//************ payer *****************

        // name of table in the mySQL database
         $dest_table = "payer";
         // name of the table in FileMaker
         $source_table = "entities";

         $search = [
           ['category' => '=Payer']
         ];
         $offset = 0;
         $column_map=[ // normalizing of column names for better readability
           "modificationHostTimestamp" => "date_modified",
           "nameFull" => "name"
         ];

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

           $update_res =  [];
           do{
              $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
              $rs_array = json_decode($rs);
              $primaryKey = $primaryKeyPairs[$dest_table];
              if (!$targetedExecution || $rdsTargeted) {
                $begin = microtime(true);
                $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              }
              if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                
                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                
              }
              $offset += $limit;
              $fquery->offset = $offset;
            }while (count((array)$rs_array) > 0 );

            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
            $log[] = [$dest_table,sumRes($update_res)];
         }


         //************ member *****************

                 // name of table in the mySQL database
                  $dest_table = "member";
                  // name of the table in FileMaker
                  $source_table = "member";

                  $search = [
                    ['type' => '=Patient']
                  ];
                  $offset = 0;
                  $column_map=[ // normalizing of column names for better readability
                    "modificationHostTimestamp" => "date_modified",
                    "nameFull" => "name"
                  ];

                  if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
                    $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

                    $update_res =  [];
                    do{
                        $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                        $rs_array = json_decode($rs);
                          $primaryKey = $primaryKeyPairs[$dest_table];
                        if (!$targetedExecution || $rdsTargeted) {
                          $begin = microtime(true);
                          $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                          $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                          $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                        }
                                                if (!$targetedExecution || $snowflakeTargeted) {
                          $begin = microtime(true);
                          $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                          $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                          
                          $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                          $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                          
                        }
                        $offset += $limit;
                        $fquery->offset = $offset;
                      }while (count((array)$rs_array) > 0 );

                      $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
                      $log[] = [$dest_table,sumRes($update_res)];
                  }



//************ request *****************

        // name of table in the mySQL database
         $dest_table = "request";
          // name of the table in FileMaker
         $source_table = "request";

         $search = [
           ['modificationHostTimestamp' => ">".$last_update],
           //['id' => '> 0'],
           ['payer_name' => 'Test', 'omit' => 'true']
         ];
         $offset = 0;
         $column_map=[
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
           "member_state" =>"state",
           "creationHostTimestamp" => "date_created",
           "modificationHostTimestamp" => "date_modified",
           "creationAccountName" => "created_by",
           "long" =>"lng",
           "amount_calc" => "amount"
         ];



         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {

           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);
          // include 'demodata.php';

           $update_res =  [];
           do{
              $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
              $rs_array = json_decode($rs);
              // $rs = $json_string;
              // $rs_array = $rawDataArray;
              $primaryKey = $primaryKeyPairs[$dest_table];
              if (!$targetedExecution || $rdsTargeted) {
                $begin = microtime(true);
                $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              }
              if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                
                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                
              }
              $offset += $limit;
              $fquery->offset = $offset;
            }while (count((array)$rs_array) > 0 );


            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
            $log[] = [$dest_table,sumRes($update_res)];
         }

         //print_r($log);
         //exit;
//************ request_subrequests *****************

        // name of table in the mySQL database
         $dest_table = "request_subrequests";
         // name of the table in FileMaker
         $source_table = "request_subrequests";

         $search = [
          ['modificationHostTimestamp' => ">".$last_update],
           ['Requests::payer_name' => 'Test', 'omit' => 'true'],
           ['additional_consideration' => "1",'omit' => 'true']
         ];
         $offset = 0;
         $column_map=[
           "modificationHostTimestamp" => "date_modified",
           "Requests::payer_name" => "payer_name",
           "Requests::assessment_id" => "assessment_id"
         ];

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
            $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

            $update_res =  [];
            do{
              $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
              $rs_array = json_decode($rs);
              $primaryKey = $primaryKeyPairs[$dest_table];
              if (!$targetedExecution || $rdsTargeted) {
                $begin = microtime(true);
                $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              }
              if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                
                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                
              }
              $offset += $limit;
              $fquery->offset = $offset;
            }while (count((array)$rs_array) > 0 );


            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
            $log[] = [$dest_table,sumRes($update_res)];
         }

         //************ request_fees *****************

                // name of table in the mySQL database
                  $dest_table = "request_fees";
                  // name of the table in FileMaker
                  $source_table = "request_fees";

                  $search = [
                   ['modificationHostTimestamp' => ">".$last_update]
                  ];
                  $offset = 0;
                  $column_map=[
                    "modificationHostTimestamp" => "date_modified",
                    "creationHostTimestamp" => "date_created",
                    "date" => "date_fee"
                  ];

                  if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
                    $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

                    $update_res =  [];
                    do{
                        $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                        $rs_array = json_decode($rs);
                        $primaryKey = $primaryKeyPairs[$dest_table];
                        if (!$targetedExecution || $rdsTargeted) {
                          $begin = microtime(true);
                          $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                          $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                          $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                        }
                        if (!$targetedExecution || $snowflakeTargeted) {
                          $begin = microtime(true);
                          $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                          $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                          
                          $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                          $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                          
                        }
                        $offset += $limit;
                        $fquery->offset = $offset;
                      }while (count((array)$rs_array) > 0 );


                      $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
                      $log[] = [$dest_table,sumRes($update_res)];
                  }

 //************invoices *****************

        // name of table in the mySQL database
         $dest_table = "invoices";
         // name of the table in FileMaker
         $source_table = "invoices";

         $search = [
           ['modificationHostTimestamp' => ">".$last_update]
         ];
         $offset = 0;
         $column_map=[
           "balancePaymentsUnapplied" => "payments_unapplied",
           "Entities Invoices Payer::nameFull" => "plan",
           "Entities Invoices Payer::billing_payment_due" => "billing_payment_due",
           "modificationHostTimestamp" => "date_modified",
           "costSubtotal" => "cost_subtotal",
           "invoiceNumber" => "invoice_number",
           "billing_period_start" => "date_billing_start"
         ];

           if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

           $update_res =  [];
           do{
              $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
              $rs_array = json_decode($rs);
              $primaryKey = $primaryKeyPairs[$dest_table];
              if (!$targetedExecution || $rdsTargeted) {
                $begin = microtime(true);
                $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              }
              if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                
                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                
              }
              $offset += $limit;
              $fquery->offset = $offset;
            }while (count((array)$rs_array) > 0 );


            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
            $log[] = [$dest_table,sumRes($update_res)];
         }

//************invoice_tiems *****************

                // name of table in the mySQL database
                 $dest_table = "invoice_items";
                 // name of the table in FileMaker
                 $source_table = "invoice_items";

                 $search = [
                   ['modificationHostTimestamp' => ">".$last_update]
                 ];
                 $offset = 0;
                 $column_map=[
                   "modificationHostTimestamp" => "date_modified",
                   "id_Request" => "id_request",
                   "id_Invoice" => "id_invoice"
                 ];

                   if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
                   $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

                   $update_res =  [];
                   do{
                      $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
                      $rs_array = json_decode($rs);
                      $primaryKey = $primaryKeyPairs[$dest_table];
                      if (!$targetedExecution || $rdsTargeted) {
                        $begin = microtime(true);
                        $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                        $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                        $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                      }
                      if (!$targetedExecution || $snowflakeTargeted) {
                        $begin = microtime(true);
                        $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                        $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                        
                        $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                        $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                        
                      }
                      $offset += $limit;
                      $fquery->offset = $offset;
                    }while (count((array)$rs_array) > 0 );


                    $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
                    $log[] = [$dest_table,sumRes($update_res)];
                 }


//************ activity *****************

      // name of table in the mySQL database
       $dest_table = "activity";
        // name of the table in FileMaker
       $source_table = "activity";

       $search = [
         ['modificationHostTimestamp' => ">".$last_update]
       ];
       $offset = 0;
       $column_map=[
         "modificationHostTimestamp" => "date_modified"
       ];

       if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
         $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

         $update_res =  [];
         do{
            $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
            $rs_array = json_decode($rs);
            $primaryKey = $primaryKeyPairs[$dest_table];
            if (!$targetedExecution || $rdsTargeted) {
              $begin = microtime(true);
              $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
              $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
              $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
            }
            if (!$targetedExecution || $snowflakeTargeted) {
              $begin = microtime(true);
              $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
              $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
              
              $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
              $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              
            }
            $offset += $limit;
            $fquery->offset = $offset;
          }while (count((array)$rs_array) > 0 );


          $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
          $log[] = [$dest_table,sumRes($update_res)];
       }


//************ locations *****************

       // name of table in the mySQL database
       $dest_table = "locations";
       // name of the table in FileMaker
       $source_table = "locations";

       $search = [
         ['modificationHostTimestamp' => ">".$last_update]
       ];
       $offset = 0;
       $column_map=[
         "modificationHostTimestamp" => "date_modified",
         "long" => "lng"
       ];
       $sort = "recordId";

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
         $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

         $update_res =  [];
         do{
            $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
            $rs_array = json_decode($rs);
            $primaryKey = $primaryKeyPairs[$dest_table];
            if (!$targetedExecution || $rdsTargeted) {
              $begin = microtime(true);
              $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
              $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
              $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
            }
            if (!$targetedExecution || $snowflakeTargeted) {
              $begin = microtime(true);
              $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
              $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
              
              $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
              $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              
            }
            $offset += $limit;
            $fquery->offset = $offset;
          }while (count((array)$rs_array) > 0 );


          $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
          $log[] = [$dest_table,sumRes($update_res)];
       }

//************ payments *****************

      // name of table in the mySQL database
       $dest_table = "payments";
       // name of the table in FileMaker
       $source_table = "payments";

       $search = [
         ['modificationHostTimestamp' => ">".$last_update]
       ];
       $offset = 0;
       $column_map=[
         "modificationHostTimestamp" => "date_modified",
         "modificationAccountName" => "modified_by"
       ];
       $sort = "recordId";

         if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
         $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

         $update_res =  [];
         do{
            $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
            $rs_array = json_decode($rs);
            $primaryKey = $primaryKeyPairs[$dest_table];
            if (!$targetedExecution || $rdsTargeted) {
              $begin = microtime(true);
              $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
              $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
              $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
            }
            if (!$targetedExecution || $snowflakeTargeted) {
              $begin = microtime(true);
              $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
              $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
              
              $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
              $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              
            }
            $offset += $limit;
            $fquery->offset = $offset;
           }while (count((array)$rs_array) > 0 );


           $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
           $log[] = [$dest_table,sumRes($update_res)];
       }

//*****************invoice_activity***********************************

      // name of table in the mySQL database
         $dest_table = "invoice_activity";
         // name of the table in FileMaker
         $source_table = "invoice_activity";

         $search = [
           ['modificationHostTimestamp' => ">".$last_update]
         ];
         $offset = 0;
         $column_map=[
           "modificationHostTimestamp" => "date_modified"
         ];
         $sort = "recordId";

           if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
           $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

           $update_res =  [];
           do{
              $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL
              $rs_array = json_decode($rs);
              $primaryKey = $primaryKeyPairs[$dest_table];
              if (!$targetedExecution || $rdsTargeted) {
                $begin = microtime(true);
                $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
              }
              if (!$targetedExecution || $snowflakeTargeted) {
                $begin = microtime(true);
                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                
                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                
              }
              $offset += $limit;
              $fquery->offset = $offset;
             }while (count((array)$rs_array) > 0 );


             $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
             $log[] = [$dest_table,sumRes($update_res)];
         }

         //************ outcomes *****************

              // name of table in the mySQL database
              $dest_table = "outcomes";
              // name of the table in FileMaker
              $source_table = "outcomes";

              $search = [
                ['id' => "> 0"]
              ];

              $offset = 0;

              if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
                $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

                $update_res =  [];
                do{
                    $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL

                    $rs_array = json_decode($rs);
                    $primaryKey = $primaryKeyPairs[$dest_table];
                    if (!$targetedExecution || $rdsTargeted) {
                      $begin = microtime(true);
                      $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                      $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                      $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                    }
                    if (!$targetedExecution || $snowflakeTargeted) {
                      $begin = microtime(true);
                      $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                      $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                      
                      $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                      $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                      
                    }
                    $offset += $limit;
                    $fquery->offset = $offset;
                  }while (count((array)$rs_array) > 0 );

                  $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
                  $log[] = [$dest_table,sumRes($update_res)];
              }

              //************ managed_companies *****************

                   // name of table in the mySQL database
                   $dest_table = "managed_companies";
                   // name of the table in FileMaker
                   $source_table = "managed_companies";

                   $search = [
                     ['id' => "> 0"]
                   ];
                   $column_map=[
                     "modificationHostTimestamp" => "date_modified"
                   ];
                   $offset = 0;

                   if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {

                     $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

                     $update_res =  [];
                     do{
                        $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL

                        $rs_array = json_decode($rs);
                        $primaryKey = $primaryKeyPairs[$dest_table];
                        if (!$targetedExecution || $rdsTargeted) {
                          $begin = microtime(true);
                          $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                          $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                          $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                        }
                        if (!$targetedExecution || $snowflakeTargeted) {
                          $begin = microtime(true);
                          $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                          $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                          
                          $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                          $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                          
                        }
                        $offset += $limit;
                        $fquery->offset = $offset;
                       }while (count((array)$rs_array) > 0 );

                       $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
                       $log[] = [$dest_table,sumRes($update_res)];
                   }
                   //************ lines of business *****************

                        // name of table in the mySQL database
                        $dest_table = "lines_of_business";
                        // name of the table in FileMaker
                        $source_table = "linesofbusiness";

                        $search = [
                         ['modificationHostTimestamp' => ">".$last_update]
                        ];
                        $column_map=[
                          "modificationHostTimestamp" => "date_modified"
                        ];
                        $offset = 0;

                        if (in_array($dest_table,$dest_table_list) && !in_array(strtolower($dest_table), $ignoreTables)) {
                          $fquery = new FM_extract($db,$host,$source_table,$dest_table,$search,$limit,$offset,$sort,$column_map);

                          $update_res =  [];
                          do{
                              $rs = $fquery->getRecordSet(); // records from FileMaker to be update in mySQL

                              $rs_array = json_decode($rs);
                              $primaryKey = $primaryKeyPairs[$dest_table];
                              if (!$targetedExecution || $rdsTargeted) {
                                $begin = microtime(true);
                                $update_res[] = (count((array)$rs_array)>0)  ? $rds->updateTable($rs, $dest_table, $primaryKey) : [0,0,0];
                                $rdsTimeLapse[$dest_table] = (!empty($rdsTimeLapse[$dest_table])) ? $rdsTimeLapse[$dest_table] : 0;
                                $rdsTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                              }
                              if (!$targetedExecution || $snowflakeTargeted) {
                                $begin = microtime(true);
                                $snowflakeAffectedRows[$dest_table] = ((!empty($snowflakeAffectedRows[$dest_table])) ? $snowflakeAffectedRows[$dest_table] : 0);
                                $snowflakeTimeLapse[$dest_table] = ((!empty($snowflakeTimeLapse[$dest_table])) ? $snowflakeTimeLapse[$dest_table] : 0);
                                
                                $snowflakeAffectedRows[$dest_table] += (count((array)$rs_array) > 0) ? $snow->updateTable($rs, $dest_table, $primaryKey) : 0;
                                $snowflakeTimeLapse[$dest_table] += number_format(((microtime(true)) - $begin), 2);
                                
                              }
                              $offset += $limit;
                              $fquery->offset = $offset;
                            }while (count((array)$rs_array) > 0 );

                            $source_list[$source_table] = $dest_table.":".count((array)$rs_array) ;
                            $log[] = [$dest_table,sumRes($update_res)];
                        }

//********************* end of tables to sync ********************


//************* log all the changes to etl_log table *************

         $endtime = microtime(true);
         $timediff = number_format($endtime - $starttime,2);

         $update_date = date_create($last_update);
         $last_update_formatted =   date_format($update_date, 'Y-m-d');

         $dest_table = "etl_log";
         $primaryKey = $primaryKeyPairs[$dest_table];
         $records = [];

         $nochange_records = 0;
         $change_records = 0;
         $add_records  = 0;
         if (!$targetedExecution || $rdsTargeted) {
         foreach($log as $res){
           $record= [];
           $record["data_table"] = $res[0];
           $record["records_modified"] =  $res[1][2];
           $record["records_added"] =  $res[1][1];
           $record["records_unchanged"] =  $res[1][0];
           $record["date_modified"] = $last_update_formatted;
           $record["date_time"] = date("Y-m-d H:i:s");
          //  $record["timelapse"] = $timediff;
           $record["timelapse"] = $rdsTimeLapse[$record["data_table"]];
           $records[] = $record;
           $nochange_records += $res[1][0];
           $change_records += $res[1][2];
           $add_records  += $res[1][1];
         }

         $total_records = ($nochange_records + $change_records + $add_records);

         $source_log = json_encode($source_list,true);

         if($total_records > 0){

           $json_log = json_encode($records,true);


           // update the etl_log table if logging is turned on

           if($logging){
             $added = $rds->updateTable($json_log, $dest_table, $primaryKey);
               //$logGroupName = 'data-warehouse';
               //$logStreamName = date('Y-m-d');
               //$cloudWatchLogger = new CloudWatchLogger($logGroupName, $logStreamName);
               //$cloudWatchLogger->log($json_log);
           }

           $prettySourceLog =  implode("\n",json_decode($source_log,true));
         //  print_r($added);
         //  print_r($json_log);
          print_r("\n");
           print_r("date_modified: >".$last_update_formatted);
           print_r("\n");
           print_r("RDS time elapsed in seconds: ");
           print_r("\n");
           print_r($rdsTimeLapse);
           print_r("\n");
           print_r("records unchanged: ".  $nochange_records);
           print_r("\n");
           print_r("records updated: ".  $change_records);
           print_r("\n");
           print_r("records added: ".  $add_records);
           print_r("\n\n");
           print_r("data_lineage: ".$prettySourceLog);
         }else{
             print_r("no RDS records added for ".$last_update_formatted);
         }
        }

        // Snowflake Logs
        if (!$targetedExecution || $snowflakeTargeted) {
          $totalAffectedRecords = array_sum($snowflakeAffectedRows);
          if ($totalAffectedRecords > 0) {
            $source_log = json_encode($snowflakeAffectedRows, true);
            foreach ($snowflakeAffectedRows as $table => $affectedRows) {
              $snowflakeRecord= [];
              $snowflakeRecord["data_table"] = $table;
              /* Unlike MySQL, even in the case of an update 
              where nothing is changed, snowflake returns it as an affected Row
              So, we can't track unchanged records separately.
              Therefore, we're only recording the affected rows returned by snowflake.
              */
              $snowflakeRecord["affected_rows"] =  $affectedRows;
              $snowflakeRecord["date_modified"] = $last_update_formatted;
              $snowflakeRecord["date_time"] = date("Y-m-d H:i:s");
              // $snowflakeRecord["timelapse"] = (int)array_sum($snowflakeTimeLapse); //this is actually the total timelapse of the script, not just snowflake
              $snowflakeRecord["timelapse"] = (int)$snowflakeTimeLapse[$table]; //this is the total timelapse for the particular table (including multiple iterations)
              $snowflakeRecords[] = $snowflakeRecord;
            }

            if($logging){
              $json_log = json_encode($snowflakeRecords,true);
              $added = $snow->updateTable($json_log, $dest_table, $primaryKey, true);
            }
 
            $prettySourceLog =  implode("\n",json_decode($source_log,true));
            echo PHP_EOL;
            print_r("date_modified: >".$last_update_formatted);
            echo PHP_EOL;
            print_r("Snowflake time elapsed in seconds: (". array_sum($snowflakeTimeLapse) . " s)");
            echo PHP_EOL;
            print_r($snowflakeTimeLapse);
            echo PHP_EOL;
            print_r("Snowflake Affected rows:");
            echo PHP_EOL;
            print_r($snowflakeAffectedRows);
            echo PHP_EOL;
            print_r("Total Snowflake records affected: ".  $totalAffectedRecords);
            echo PHP_EOL;
            print_r("data_lineage: ".$prettySourceLog);
          }
        } else {
          print_r("No Snowflake Records affected - " . $last_update_formatted);
        }

        print_r("\n");
        print_r("Total timelapse in seconds: " . $timediff . " s");
        
         


?>
