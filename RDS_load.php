<?php

include "connectRDS.php";



class RDS_load implements Loader {
	use LogTrait, LoaderCommonsTrait;

	public $rawData;
	public $table;
	public $conn;
	public $rds_conn;
	public $dbconn;
	public $myconn;
	public $createTableQuery = ""; //variable to hold the create table query
	public $columns = [];
	public $primaryKeyColumn = null;
	public $offsetDatetime = null;

	public function __construct($config){
		$c = $this->conn =  new createCon($config);
		$rc = $this->rds_conn = $this->conn->connect();
		$this->dbconn = $rc;
		$this->logFilePath = $config['rdsLogFilePath'] ?? __DIR__ . '/rds.log';
        $this->queryLogsEnabled = $config['rdsQueryLogsEnabled'] ?? false;
			}

	/**
	 * Returns the create table query that was generated by the generateCreateTableQuery method.
	 *
	 * @return string
	 */
	public function getCreateTableQuery(): string
	{
		return $this->createTableQuery;
	}
	
	/**
	 * Clears the create table query that was generated by the generateCreateTableQuery 
	 * method, possibly in an earlier execution.
	 *
	 * @return void
	 */
	public function clearCreateTableQuery(): void
	{
		$this->createTableQuery = '';
	}
	/**
	 * Sets the name of the last table that was processed.
	 *
	 * @return void
	 */
	public function setLastProcessedTable(string $table = ''): void
	{
		$this->lastProcessedTable = $table;
	}

	/**
	 * Returns the name of the last table that was processed.
	 *
	 * @return string
	 */
	public function getLastProcessedTable(): string
	{
		return $this->lastProcessedTable;
	}

	/**
     * Function to reset the table columns. 
     * This should be triggered each time a new table is being updated, 
     * so that the same columns aren't cached for the next table being updated. 
     *
     * @return void
     */
    public function cleanTableColumns(): void
    {
        $this->columns = [];
    }

	/**
	 * Returns the data type of the received value.
	 *
	 * @param mixed $value The value whose data type is to be determined.
	 * @return string
	 */
	public function getDataType(mixed $value): string
	{
		if (is_int($value)) {
			return 'INT';
		} elseif (is_float($value)) {
			return 'DECIMAL(10,4)'; // Adjust precision as needed
		} elseif (is_bool($value)) {
			return 'TINYINT(1)'; // Use TINYINT(1) to store boolean values
		} elseif ($value === null) {
			return 'NULL'; // Use NULL type for null values
		} else {
			// return 'VARCHAR(255)';
			// Not using VARCHAR(255) because some values could be longer than 255 characters
			return 'TEXT';
		}
	}

	/**
	 * Creates a table with the recieved primaryKey parameter
	 * This method assumes that the $table & $columns properties have already been set.
	 * The table name should already have been received in the updateTable method.
	 * Similarly, the columns information should already have been extracted from the data received in the updateTable method.
	 *
	 * @param string $primaryKey The name of the primary key column.
	 * @return void
	 */
	public function createTableIfNotExists(string $primaryKey = ''): void
	{
		$this->generateCreateTableQuery($primaryKey);
		// $start = microtime(true);
		// $this->verboseLog("Create Table '" . $this->table . "' if not exists - BEGIN" . PHP_EOL);

		// $this->verboseLog("" . PHP_EOL . $this->getCreateTableQuery() . PHP_EOL);

		if ($this->dbconn->query($this->getCreateTableQuery()) === TRUE && $this->dbconn->errno == 0 ) {
			// Query has executed successfully, and no error has been returned
			if ($this->isDebugLoggingEnabled()) {
				if ($this->queryLogsEnabled) {
					$this->log($this->getCreateTableQuery(), true);
				}
				$logMessage = "Table {$this->table} created successfully.";
				$this->log('', true, $logMessage, self::$LOGTYPES['TASK_SUMMARY']); // not logging for successful tasks
			}
		} else {
			if ($this->queryLogsEnabled) {
				$this->log($this->getCreateTableQuery(), false);
			}
			echo "Error creating table: " . $this->dbconn->error;
			$logMessage = "Error creating table (`{$this->table}`): " . $this->dbconn->error . PHP_EOL;
			$this->log('', false, $logMessage, self::$LOGTYPES['TASK_SUMMARY']);
		}

		// $this->verboseLog("Create Table '" . $this->table . "' if not exists - END (" . number_format((microtime(true) - $start), 4) . " s)" . PHP_EOL);
	}

	/**
	 * This method actually generates the create table query and sets the $createTableQuery property, 
	 * based on the $table & $columns properties.
	 * This method assumes that the $table & $columns properties have already been set.
	 * The table name should already have been received in the updateTable method.
	 * Similarly, the columns information should already have been extracted from the data received in the updateTable method.
	 *
	 * @param string $primaryKey The name of the primary key column.
	 * @return void
	 */
	public function generateCreateTableQuery(string $primaryKey = null): void
	{
		$this->clearCreateTableQuery();
		
        // Make sure no column has been repeated twice, this will mess up the create table query
        array_unique($this->columns);

		// If the primary key column is TEXT, change it to VARCHAR(255), 
		// since a fixed needs to be provided for string typed primary keys
		if (
		!empty($primaryKey) 
		&& !empty($this->columns[$primaryKey]) 
		&& $this->columns[$primaryKey] == 'TEXT'
		) {
			$this->columns[$primaryKey] = 'VARCHAR(255)';
		} else if (!empty($primaryKey) && empty($this->columns[$primaryKey])) {
			// If the primary key column is not present in the columns array, add it as an auto-incrementing integer
			$this->columns[$primaryKey] = 'INT AUTO_INCREMENT';

		}

		$this->createTableQuery = "CREATE TABLE IF NOT EXISTS " . $this->table . " (";

		$this->createTableQuery .= implode(", ", array_map(function ($columnName, $columnType) {
			return "$columnName $columnType";
		}, array_keys($this->columns), $this->columns));
		
		// Add the primary key if it has been provided
		if (!empty($primaryKey)) {
			$this->createTableQuery .= ", PRIMARY KEY ($primaryKey)";
		}
		
		$this->createTableQuery .= ");";
	}

	/**
	 * Extracts the columns and their respecitve data types from the parsed array (of received json data).
	 *
	 * @param array $data The parsed array (of received json data).
	 * @return void
	 */
	public function extractTableColumnsAndDataTypes(array $data): void
	{
		// $start = microtime(true);
		// $this->verboseLog("Extracting columns from incoming data - BEGIN");
		
		// Clean the columns array property
		$this->cleanTableColumns();
		
		foreach ($data as $key => $value) {
			$this->columns[$key] = $this->getDataType($value);
		}

		// $this->verboseLog("Extracting columns from incoming data - END (" . number_format((microtime(true) - $start), 7) . " s)");
	}

	
	/**
	 * This table flushes the existing columns property against the target table
     * and repopulates them based on the description of the table from RDS.
     * We haven't used a select query, as the target table will be empty on first creation.
	 * The method populates the $columns property of the class with field names as keys and data types as values.
	 * The $primaryKeyColumn property is also set if a primary key is found.
	 *
	 * @return void
	 */
	public function getTargetTableColumns(): void
	{
		// $start = microtime(true);
		// $this->verboseLog("Get Target Table Columns - BEGIN");

		$query = "SHOW COLUMNS FROM {$this->table}";
		$result = $this->dbconn->query($query);
		if ($result->num_rows > 0) {
			// Clean the columns array property if any results have been returned
			$this->cleanTableColumns();
			
			while ($row = $result->fetch_assoc()) {
				$this->columns[$row['Field']] = $row['Type'];
				// Also set the $primaryKeyColumn property if primary key is found
				if ($row['Key'] == 'PRI') {
					$this->primaryKeyColumn = $row['Field'];
				} 
			}
		}

		// $this->verboseLog("Get Target Table Columns - END (" . number_format((microtime(true) - $start), 4) . " s)");
	}

	function updateTable($rawData,$table, $primaryKeyColumn = ''){
    	$start = microtime(true);
		$this->verboseLog(PHP_EOL . "======================================" . PHP_EOL . "RDS - updateTable Process for `{$table}` - BEGIN" . PHP_EOL);
		
    $this->rawData = $rawData;
    // $this->$table = $table;
    $this->table = $table;
    $dbconn = $this->dbconn;

    //print_r($rawData);
    //exit;

    $jsonData = json_decode($rawData,true);

      if(!isset($table)){
        foreach($jsonData[0] as $key => $value){
        	$table = $key;
        }
        $jsonData = $jsonData[$table];
      }

    // Table creation portion - BEGIN
     //Is this a new table being processed, or are we processing the same table?
		$isNewTableHandling = (empty($this->lastProcessedTable) || $this->lastProcessedTable !== $table) ;
		if ($isNewTableHandling) {
			$this->clearCreateTableQuery();
			$this->cleanTableColumns();
			$this->primaryKeyColumn = $primaryKeyColumn;

			/* Assuming $jsonData is an associative array where keys are column names and values are their values,
			create an array named $columns, with column names as indexes, and data types (based on values) as values */
			// Extract the columns and their respecitve data types from the parsed array (of received json data)
			$this->extractTableColumnsAndDataTypes($jsonData[0]);
			
			// Attempt to create table if it doesn't already exist
			$this->createTableIfNotExists($primaryKeyColumn);

			// Fetch updated columns from the RDS database
			$this->getTargetTableColumns();
		}
    // Table creation portion - END

      $i = 0;

      $clist = [];
      $sql = [];

    //   $this->verboseLog("Columns extraction - BEGIN");
      foreach($jsonData[0] as $c => $v){
      //	$clist[] = $c;
        array_push($clist,$c);
      }
    //   $this->verboseLog("Columns extraction - END");


      $cols =  "(" . implode(',',$clist) . ")";
    //   $this->verboseLog("SQL Queries building - BEGIN");
      foreach($jsonData as $row ){

      	$update = [];
      	$vlist = [];
        $va = [];
        $fa = [];

      	foreach($row as $f => $v){
          $str = preg_replace('/\x{FEFF}/u', '', $v);
          $va[] = $str;
          $fa[] = $f;
      		$vlist[] =  mysqli_real_escape_string($dbconn, $str );
      		$update[] = $f."='". mysqli_real_escape_string($dbconn, $str ) ."'";
      	}


        if(count($vlist) != count($clist)){
          //echo(count($vlist).":". count($clist) );
          var_dump(array_diff($clist,$fa));

          exit;
        }
      	$updateSQL = " " . implode(" , ",$update) ;//. $where;

      	$values = " Values('".implode("','",$vlist)."')";

      	$sql[] = "INSERT INTO $table  $cols  $values ON DUPLICATE KEY UPDATE $updateSQL ";

      }
    //   $this->verboseLog("SQL Queries building - END");

         $added = [];
         $added[0] = 0;
         $added[1] = 0;
         $added[2] = 0;
        //   $this->verboseLog("Records Processing - BEGIN");
          $counter = 0;
          $totalStatements = count($sql);
          foreach($sql as $s){
            if ($dbconn->query($s) === TRUE && $dbconn->errno == 0) {
              
				$added[$dbconn->affected_rows] ++;
				$logMessage = "Successfully processed: " . ($counter + 1) . "/" . $totalStatements .
					"(" . (($counter + 1)/$totalStatements) * 100 ."%) records";
				
				// $this->verboseLog($logMessage,
				// 	true
				// );
				
				if ($this->isDebugLoggingEnabled()) {
					// Line-wise logging of queries
					$this->log('', true, $logMessage, self::$LOGTYPES['TASK_SUMMARY']);
				}
			} else {
				$logMessage = "Error inserting new records into table (`{$this->table}`): " . $this->dbconn->error;
				if ($this->queryLogsEnabled) {
					$this->log($s, false, $this->dbconn->error);
				}
				$this->log('', false, $logMessage, self::$LOGTYPES['TASK_SUMMARY']);
			}
			$counter++;
		}
		// $this->verboseLog("Records Processing - END");

		$this->verboseLog("RDS - updateTable Process for table `{$table}` - END (" . number_format((microtime(true) - $start), 4) . " s)" . 
		PHP_EOL . "======================================" . PHP_EOL . PHP_EOL);
	
		$this->setLastProcessedTable($table);
		return $added;

	}

	/**
	 * Get the last synced timestamp for a specific table.
	 *
	 * @param string $table The name of the table.
	 * @return string|null The last synced timestamp, or null if no record was found.
	 */
	public function getLastSyncedTimestamp($table)
	{
		// Make sure the sync_status table exists, and create it if it doesn't
		$this->ensureSyncStatusTableExists();

		// Prepare the SQL query
		$sql = "SELECT last_synced_timestamp FROM sync_status WHERE table_name = ?";

		// Prepare the statement
		$stmt = $this->dbconn->prepare($sql);
		$stmt->bind_param('s', $table);
		$stmt->execute();

		// Bind the result
		$stmt->bind_result($last_synced_timestamp);
		$stmt->fetch();
		$stmt->close();

		// Return the last synced timestamp, or null if no record was found
		return isset($last_synced_timestamp) ? $last_synced_timestamp : null;
	}

	/**
	 * Ensure that the sync_status table exists in the database.
	 * If the table doesn't exist, it will be created.
	 *
	 * @return void
	 */
	public function ensureSyncStatusTableExists()
	{
		// SQL query to check if the sync_status table exists
		$sql = "SHOW TABLES LIKE 'sync_status'";

		// Execute the query
		$result = $this->dbconn->query($sql);

		// If the sync_status table doesn't exist, create it
		if ($result->num_rows == 0) {
			$sql = "
				CREATE TABLE sync_status (
					table_name VARCHAR(255) NOT NULL,
					last_synced_timestamp TIMESTAMP NOT NULL,
					PRIMARY KEY (table_name)
				)
			";

			// Execute the query
			$this->dbconn->query($sql);
		}
	}

	/**
	 * Update the last synced timestamp for a specific table.
	 *
	 * @param string $table The name of the table.
	 * @param string $timestamp The timestamp to be set as the last synced timestamp.
	 * @return void
	 */
	public function updateLastSyncedTimestamp($table, $timestamp)
	{
		// Make sure the sync_status table exists, and create it if it doesn't
		$this->ensureSyncStatusTableExists();

        // convert timestamp to mysql acceptable timestamp (Y-m-d H:i:s)
        $timestamp = date('Y-m-d H:i:s', strtotime($timestamp));

		// Prepare the SQL query
		$sql = "INSERT INTO sync_status (table_name, last_synced_timestamp) VALUES (?, ?) ON DUPLICATE KEY UPDATE last_synced_timestamp = ?";

		// Prepare the statement
		$stmt = $this->dbconn->prepare($sql);
		$stmt->bind_param('sss', $table, $timestamp, $timestamp);
		$stmt->execute();
		$stmt->close();
	}

	/**
	 * Set the offset datetime.
	 *
	 * @param string $offsetDatetime The offset datetime to set.
	 * @return void
	 */
	public function setOffsetDatetime($offsetDatetime)
	{
		$this->offsetDatetime = $offsetDatetime;
	}

	/**
	 * Get the offset datetime.
	 *
	 * @return string The offset datetime.
	 */
	public function getOffsetDatetime()
	{
		return $this->offsetDatetime;
	}
}

?>