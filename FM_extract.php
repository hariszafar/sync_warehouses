<?php

/********************************************
Modified script by Ben Marchbanks ben@dme-cg.com or ben@alqemy
 Phone: 540 760 4104

This script performs a query in FileMaker Database and transforms
some of the data for formatting and naming conventions
    1. Date format in FileMaker is d/m/y - transformed to Y-m-d
    2. Data from relationships includes long crypted names for the columns
      these are mapped to shorter column names for readbility
    3. Collections are created for use as value-list or indices
    4. Layouts in FM used for queries should be named table name+ "_data_warehouse"

***********************************************/


// uses session to store data api token
if (defined('PHP_SAPI') && 'cli' === PHP_SAPI) {
  if (!isset($GLOBALS['_SESSION'])) {
    $GLOBALS['_SESSION'] = [];
  }
}
elseif (session_status() == PHP_SESSION_NONE) {
  session_start();
  header("Access-Control-Allow-Origin: *");
}

ini_set('max_execution_time', '0'); // for infinite time of execution

ini_set('display_errors', 1);
error_reporting(E_ERROR | E_PARSE | E_WARNING | E_ALL);


require_once(__DIR__ . DIRECTORY_SEPARATOR . 'lib/FmRest.php');
require_once(__DIR__ . DIRECTORY_SEPARATOR . 'lib/FmdataManager.php');
require_once(__DIR__ . DIRECTORY_SEPARATOR . 'lib/FmdataTrait.php');
require_once(__DIR__ . DIRECTORY_SEPARATOR . 'lib/connectFM.php');


class  FM_extract{

  public $db;
  public $host;
  public $table;
  public $tablesSuffix = "";
  public $dest_table; // this property is never even used in the class/script
  public $sort;
  public $search;
  public $limit = 100;
  public $offset = 0;
  public $dbse;
  public $column_map;


  public function __construct($db,$host,$table,$dest_table,$search,$limit,$offset,$sort,$column_map)
  {
      global $config;
      $this->db = $db ;
      $this->host = $host;
      $this->table = $table;
      $this->dest_table = $dest_table;
      $this->search = $search;
      $this->limit =$limit;
      $this->offset = $offset;
      $this->sort = $sort;
      $this->column_map = $column_map;


      $this->dbse = new connectFM([
          'fmdb_database' => $this->db,
          'fmdb_host' => $this->host // private network ip for stage server
      ]);
      $this->tablesSuffix = $config['fm_tables_suffix'] ?? "_data_warehouse";

      if (!$this->dbse->login($config['fm_user'] ?? 'webconnect', $config['fm_password'] ?? 'snQTX6JNPh4eTnmn')) {
         var_dump($host. " Error logging in: ".$db, $this->dbse->getLastError());
        exit;
      }
      // else{
      //   print_r("login successful");
      //   exit;
      // }

  }
  function __destruct() {
      //print "Destroying " . __CLASS__ . "\n";
  }

  function getRecordSet(){

    $db = $this->db ;
    $host = $this->host;
    $table = $this->table;
    $dest_table = $this->dest_table;
    $search = $this->search;
    $limit = $this->limit;
    $offset = $this->offset;
    $sort = $this->sort;
    $column_map =$this->column_map;
    $dbse = $this->dbse;

        $norecords = false;

        // $layout = $table."_data_warehouse"; //TODO: this is a convention for the layout name in the production FM
        $layout = $table . $this->tablesSuffix; // Replaced with suffix from config.php (being populated from ENV vars)
        // $layout = $table;


        $query = [];
        $query = ['query' => $search];
        $query['limit'] = $limit;

        if ($offset) {
          $query['offset'] = $offset;
        }

        // sorting wasn't even implemented before - added code here to add sorting to the query
        if (is_array($sort) && !empty($sort)) {
          $query['sort'] = $sort;
        }

        // the layout can be passed to this function as 2nd param
        if (null === ($data = $dbse->query($query, $layout))) {

          $norecords = true;
           //print_r($query);
           //print_r($table ." records not found! " . $layout . " " . ($dbse->getLastError()));
           //exit;
        }
        else {
         $results = $data["data"];


        //print_r(json_encode($data, true));
        //return;
            // object containing all output data;
          $data_out = [];

           //list of records;
          $data_out['records'] = [];

            // iterate over all records and reformat date, build collections....
            forEach($results as $row){
                $line = [];

                forEach($row as $col => $value){
                  $value = trim($value);
                  $col = (isset($column_map[$col])) ? $column_map[$col] : $col;

                // for columns which are dates add them to a dates collection
                  if(explode("_",$col)[0] == "date"){

                    if($value != "" && strtotime($value) !== false){

                      $date0 =  date_create($value);

                      $date2 = date_format($date0, 'Y-m-d');
                      $date1 = date_format($date0, 'Y-m-d H:i:s');

                      $date = (strpos($value,":" )) ? $date1 :$date2;

                      $line[$col] = $date;
                    }else{
                      $line[$col] ="";
                    }

                  }else{
                    $column = str_replace(" « "," ",$col);

                    $c =  (isset($column_map[$column])) ? $column_map[$column] : $col;
                    if($c=="state"){
                      strtoupper($value);
                    }
                    $line[$c] = $value;
                  }

                }

                $data_out['records'][] =  $line;

            }

          return json_encode($data_out["records"], true);

        }
      }

}

?>
