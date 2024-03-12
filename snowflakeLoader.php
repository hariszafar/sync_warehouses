<?php

include 'snowflakeConfig.php';

class SnowflakeLoader implements Loader {
    use LogTrait;
    
    public $rawData;
    public $rds_conn;
    
    public $snowflakeAccount;
    public $snowflakeUser;
    public $snowflakePassword;
    public $snowflakeSchema;
    public $snowflakeDatabase;
    public $snowflakeWarehouse;
    public $snowflakeBackupDatabase;
    public $snowflakeBackupSchema;
    
    
    public $conn = null;
    public $affectedRows = 0;
    public $table;
    public $backupTableName = '';
    public $tableCreationStatus = '';
    public $syncTableName = "etl_log";
    public $archiveSyncTable = false;
    public $createTableQuery = ""; //variable to hold the create table query
    public $temporaryTable = null;
    public $temporaryTableQuery = "";
    public $lastProcessedTable = null;
    public $columns = [];
    public $primaryKeyColumn = null;
    public $columnDataTypes = []; 

    public $errorInfo = null;
    public $errorMessage = null;

    public $tableKeyIsAutoIncrement = false;

    public $insertChunkSize = 1;
    public $chunkStringSizes = []; //Record the string size of each chunk
    public $chunkRowCounts = []; // Record the number of rows in each chunk
    public $offsetDatetime = null;

    /**
     * Expected Connection Parameters for Snowflake PDO connection
     * $options array [
     *      'account' => account string for connectivity,
     *      'user' => username,
     *      'password' => password,
     *      'schema' => the schema to use,
     *      'database' => the database to connect to,
     *      'warehouse' => the associated warehouse,
     *      'queryLogsEnabled' => whether to query logging for snowflake is enabled,
     *      'logFilePath' => the snowflake log file path,
     *      'insertChunkSize' => the number of records to attempt to insert in a single insert statement (for bulk insertion),
     *      ]
     * Fallback data is received from .\snowflakeConfig.php - make sure to configure it.
     */
    public function __construct(array $options = [])
    {
        $this->snowflakeAccount = $options['account'] ?? SNOWFLAKE_ACCOUNT;
        $this->snowflakeUser = $options['user'] ?? SNOWFLAKE_USER;
        $this->snowflakePassword = $options['password'] ?? SNOWFLAKE_PASSWORD;
        // $this->snowflakeTable = $options['table'] ?? '';
        $this->snowflakeSchema = $options['schema'] ?? SNOWFLAKE_SCHEMA;
        $this->snowflakeDatabase = $options['database'] ?? SNOWFLAKE_DATABASE;
        $this->snowflakeWarehouse = $options['warehouse'] ?? SNOWFLAKE_WAREHOUSE;
        $this->snowflakeBackupDatabase = $options['backup_database'] ?? SNOWFLAKE_BACKUP_DATABASE;
        $this->snowflakeBackupSchema = $options['backup_schema'] ?? SNOWFLAKE_BACKUP_SCHEMA;
        
        if (!empty($this->snowflakeAccount) && !empty($this->snowflakeUser) && !empty($this->snowflakePassword)) {
            $this->establishConnection();
        }

        //if connection has been established, attempt to point cursor to the relevant WH, DB & Schema
        if (!empty($this->conn)) {
            if (!empty($this->snowflakeWarehouse)) {
                $this->useWarehouse($this->snowflakeWarehouse);
            }
            if (!empty($this->snowflakeDatabase)) {
                $this->useDatabase($this->snowflakeDatabase);
            }
            if (!empty($this->snowflakeSchema)) {
                $this->useSchema($this->snowflakeSchema);
            }
        }
        
        // Log to the 'snowflake.log' file in the same directory
        $this->logFilePath = $options['logFilePath'] ?? SNOWFLAKE_LOG_FILE ?? __DIR__ . '/snowflake.log';
        $this->queryLogsEnabled = $options['queryLogsEnabled'] ?? SNOWFLAKE_LOGS_ENABLED ?? false;
        $this->insertChunkSize = $options['insertChunkSize'] ?? SNOWFLAKE_INSERT_CHUNK_SIZE ?? $this->insertChunkSize ?? 1; //fallback to one insert at a time
    }

    /**
     * Adds the size of the current chunk to the chunkStringSizes array.
     *
     * @param string $chunkSize The chunk whose size/length is to be recorded.
     * @return void
     */
    public function addChunkLengths(int $chunkSize) {
        $this->chunkStringSizes[] = strlen($chunkSize);
    }

    /**
     * Clears the chunkStringSizes array.
     *
     * @return void
     */
    public function cleanChunkLengths(): void
    {
        $this->chunkStringSizes = [];
    }

    /**
     * Adds the number of rows in the given chunk to the chunkRowCounts array.
     *
     * @param int $count The row count to be recorded for the current chunk.
     * @return void
     */
    public function addChunkRowCount(int $chunk): void
    {
        $this->chunkRowCounts[] = $chunk;
    }

    /**
     * Sets the flag to indicate whether the sync table should be archived.
     *
     * @param bool $archive Whether to archive the sync table or not. Default is false.
     * @return void
     */
    public function setArchiveSyncTableFlag(bool $archive = false): void
    {
        $this->archiveSyncTable = $archive;
    }

    /**
     * Checks if the flag to archive the sync table is set.
     *
     * @return bool True if the sync table should be archived, false otherwise.
     */
    public function isArchiveSyncTableSet(): bool
    {
        return $this->archiveSyncTable;
    }

    /**
     * Claculate and return the average chunk length for the $chunkStringSizes array.
     * The last value in the array is ignored, if the size of the last chunk is not complete.
     * 
     * @return int
     */
    public function getAverageChunkLength(): int
    {
        $chunkStringSizes = $this->chunkStringSizes;
        $chunkStringSizesCount = count($chunkStringSizes);
        $totalChunkLength = 0;
        $averageChunkLength = 0;
        if ($chunkStringSizesCount > 0) {
            $lastChunkSize = $chunkStringSizes[$chunkStringSizesCount - 1];
            if ($lastChunkSize < $this->getInsertChunkSize()) {
                $chunkStringSizesCount--;
            }
            foreach($chunkStringSizes as $chunkSize) {
                $totalChunkLength += $chunkSize;
            }
            $averageChunkLength = $totalChunkLength / $chunkStringSizesCount;
        }
        return $averageChunkLength;
    }

    /**
     * Returns the insertChunkSize parameter,
     * which is used to control how many records are attempted to be inserted together in a single chunk.
     *
     * @return integer
     */
    public function getInsertChunkSize(): int
    {
         return $this->insertChunkSize;
    }

    /**
     * Set the insertChunkSize parameter,
     * which is used to control how many records are attempted to be inserted together in a single chunk.
     *
     * @param integer $insertChunkSize
     * @return void
     */
    public function setInsertChunkSize(int $insertChunkSize): void
    {
        $this->insertChunkSize = $insertChunkSize;
    }

    /**
     * This method generates a unique name for the temporary table (to avoid conflicts on Snowflake)
     *
     * @return void
     */
    public function generateTemporaryTableName(): void
    {
        $this->temporaryTable = $this->table . '_' . time();
    }

    /**
     * Returns the name for the temporary table
     *
     * @return string
     */
    public function getTemporaryTableName(): string
    {
        if (empty($this->temporaryTable)) {
            $this->generateTemporaryTableName();
        }
        return $this->temporaryTable;
    }

    /**
     * Clears the temporaryTableQuery parameter
     *
     * @return void
     */
    public function clearTemporaryTableQuery(): void
    {
        $this->temporaryTableQuery = "";
    }
    
    /**
     * Returns the temporaryTableQuery parameter, which holds the sql query string for creating the temporary table
     *
     * @return string
     */
    public function getTemporaryTableQuery(): string
    {
        return $this->temporaryTableQuery ?? ""; 
    }

    /**
     * Clears the createTableQuery parameter
     *
     * @return void
     */
    public function clearCreateTableQuery(): void
    {
        $this->createTableQuery = "";
    }

    /**
     * Returns the createTableQuery parameter, which holds the sql query string for creating the main table
     *
     * @return string
     */
    public function getCreateTableQuery(): string
    {
        return $this->createTableQuery ?? "";
    }
    
    /**
     * Establish PDO connection and connect wth Snowflake
     * 
     * @return void
     */
    public function establishConnection()
    {
        try {
            //code...
            $this->conn = new PDO("snowflake:account={$this->snowflakeAccount}", $this->snowflakeUser, $this->snowflakePassword);
            //   $dbh->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
            $this->conn->setAttribute( PDO::ATTR_CURSOR, PDO::CURSOR_SCROLL );
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    /**
     * Function to execute a prepared PDO Statement
     *
     * @param string $query  (E.g. "SELECT * FROM users WHERE username = :username AND email = :email")
     * @param array $parameters  (E.g. [ "username" => "john_doe", "email" => "john@example.com"])
     * @return mixed
     */
    public function executeQuery(string $query, ?array $parameters = null): mixed
    {
        $this->lastQuery = $query;
        $this->lastQueryParams = $parameters;
        $this->errorInfo = null;
        $this->errorMessage = null;
        try {
            $stmt = $this->conn->prepare($query);
    
            if (!empty($parameters) && count($parameters) > 0) {
                foreach($parameters as $paramKey => $paramValue) {
                    $stmt->bindParam(":{$paramKey}", $paramValue, PDO::PARAM_STR);
                }
            }
    
            if ($stmt->execute()) {
                // if ($this->queryLogsEnabled && $this->isDebugLoggingEnabled()) {
                //     $this->log($query, true);
                // }
            } else {
                // Query execution failed
                $this->errorInfo = $errorInfo = $stmt->errorInfo();
                $this->errorMessage = $errorMessage = isset($errorInfo[2]) ? $errorInfo[2] : 'Unknown error';

                $this->log($query, false, $errorMessage);
            }
    
            // Check if there are results
            if ($stmt->rowCount() > 0) {
                $this->affectedRows = $stmt->rowCount();
                // Fetch the results
                return $stmt->fetchAll(PDO::FETCH_ASSOC);
            } else {
                return [];
            }

        } catch (\Throwable $th) {
            throw $th;
        }
    }

    /**
     * Switch to specific Warehouse
     *
     * @param string|null $warehouse
     * @return void
     */
    public function useWarehouse(string $warehouse = null): void
    {
        try {
            $this->createWarehouseIfNotExists();
            $query = "Use WAREHOUSE " . ($warehouse ?? $this->snowflakeWarehouse);
            $this->executeQuery($query);
        } catch (\Throwable $th) {
            throw $th;
        }
    }
    
    /**
     * This method creates the relevant Warehouse if it doesn't already exist on Snowflake.
     *
     * @param string|null $warehouse The name of the warehouse to be created
     * @return void
     */
    public function createWarehouseIfNotExists(string $warehouse = null): void
    {
        try {
            $query = "CREATE WAREHOUSE IF NOT EXISTS " . ($warehouse ?? $this->snowflakeWarehouse);
            $this->executeQuery($query);
        } catch (\Throwable $th) {
            throw $th;
        }
    }
    
    /**
     * Switch to specific Database
     *
     * @param string|null $database
     * @return void
     */
    public function useDatabase(string $database = null): void
    {
        try {
            $this->createDatabaseIfNotExists();
            $query = "Use DATABASE " . ($database ?? $this->snowflakeDatabase);
            $this->executeQuery($query);
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    /**
     * This method creates the relevant Database if it doesn't already exist on Snowflake.
     *
     * @param string|null $database The name of the database to be created
     * @return void
     */
    public function createDatabaseIfNotExists(string $database = null): void
    {
        try {
            $query = "CREATE DATABASE IF NOT EXISTS " . ($database ?? $this->snowflakeDatabase);
            $this->executeQuery($query);
        } catch (\Throwable $th) {
            throw $th;
        }
    }
    
    /**
     * Switch to specific Schema
     *
     * @param string|null $schema
     * @return void
     */
    public function useSchema(string $schema = null): void
    {
        try {
            $this->createSchemaIfNotExists();
            $query = "Use SCHEMA " . ($schema ?? $this->snowflakeSchema);
            $this->executeQuery($query);
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    /**
     * This method will create the respective Schema, if it doesn't already exist on Snowflake
     *
     * @param string|null $schema The name of the schema to be created
     * @return void
     */
    public function createSchemaIfNotExists(string $schema = null): void
    {
        try {
            $query = "CREATE SCHEMA IF NOT EXISTS " . ($schema ?? $this->snowflakeSchema);
            $this->executeQuery($query);
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    /**
     * Method to determine the data type of the column data, for Snowflake mapping 
     *
     * @param mixed $columnValue
     * @return string
     */
    public function getSnowflakeDataType(mixed $columnValue): string
    {
        if (is_float($columnValue)) {
            return "FLOAT";
        } elseif (is_int($columnValue)) {
            return "INTEGER";
        } elseif (is_bool($columnValue)) {
            return "BOOLEAN";
        } elseif (is_string($columnValue)) {
            return "STRING";
        } elseif (is_null($columnValue)) {
            return "VARIANT"; // Assuming NULL values are allowed
        } else {
            return "VARIANT"; // Fallback for other data types
        }
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
        $this->tableKeyIsAutoIncrement = false;
        $this->primaryKeyColumn = null;
    }

    /**
     * Creates a table schema from raw data.
     *
     * @param string $rawData The raw data to create the table schema from.
     * @param string $table The name of the table.
     * @param string $primaryKeyColumn The name of the primary key column (optional).
     * @param bool $dropCreate Whether to drop and recreate the table (optional, default: false).
     * @param bool $backupPreviousTable Whether to backup the previous table (optional, default: false).
     * @return bool Returns true if the table schema was created successfully, false otherwise.
     */
    public function createTableSchemaFromData(string $rawData, string $table, string $primaryKeyColumn = '', bool $dropCreate = false, bool $backupPreviousTable = false): bool
    {
        try {
            $start = microtime(true);
            $this->verboseLog(PHP_EOL . "======================================" . PHP_EOL . 
            "Snowflake - createTableSchemaFromData Process for `{$table}` - BEGIN");

            // Run cleanup processes first
            $this->rawData = $rawData;

            // Clean up properties and prepare for the new table
            $this->backupTableName = '';
            $this->errorInfo = null;
            $this->errorMessage = null;
            $this->cleanTableColumns();
            $this->table = $table;
            $this->primaryKeyColumn = $primaryKeyColumn;

            $jsonData = json_decode($rawData,true);
            // Following same logic as in RDS_load
            if (!isset($table)) {
                foreach ($jsonData[0] as $key => $value){
                    $table = $key;
                }
                $jsonData = $jsonData[$table];
            }

            $this->extractTableColumnsAndDataTypes($jsonData[0]);

            // Check if this table already exists in the datawarehouse
            $tableExists = $this->tableExists($table);

            if ($tableExists) {
                // Backup the previous table if required
                if ($backupPreviousTable) {
                    $this->backupTable($table);
                }

                // Switch back to the main database and schema
                $this->useDatabase($this->snowflakeDatabase);
                $this->useSchema($this->snowflakeSchema);

                // Drop the table if required
                if ($dropCreate) {
                    $this->dropTable($table);
                }
            }

            $this->createTargetTableIfNotExists($primaryKeyColumn);

            /* 
                We're doing this as it was noticed thatdata import was inconsistent. 
                Data against same table was observed to have less columns, 
                which caused temporary table to  be created with less columns, resulting in an 'invalid identifier' error.
            */ 
            $this->getTargetTableColumns(); // this method also updates the schema if new columns are encountered

            $this->verboseLog(PHP_EOL . "Snowflake - createTableSchemaFromData Process for table {$table} - END ("
                . number_format((microtime(true) - $start), 4) . "s)" . PHP_EOL 
                . "======================================" . PHP_EOL);
            return true;
        } catch (\Throwable $th) {
            $this->log('', false, "Exception encountered in " . __FUNCTION__ ." method. " .$th->getMessage());
            return false;
        }
    }

    /**
     * Checks if a table exists in the Snowflake database.
     *
     * @param string $table The name of the table to check.
     * @return bool Returns true if the table exists, false otherwise.
     */
    public function tableExists(string $table): bool
    {
        $query = "SHOW TABLES LIKE '{$table}'";
        $result = $this->executeQuery($query);
        return (count($result) > 0);
    }

    /**
     * Drops a table from the Snowflake database if it exists.
     *
     * @param string $table The name of the table to be dropped.
     * @return bool Returns true if the table was dropped successfully, false otherwise.
     */
    public function dropTable(string $table): bool
    {
        $query = "DROP TABLE IF EXISTS {$table}";
        $this->executeQuery($query);
        
        // check if there were any errors, and return true or false
        return (empty($this->errorInfo) && empty($this->errorMessage));
    }

    /**
     * Creates a backup of a table in Snowflake.
     *
     * @param string $table The name of the table to be backed up.
     * @return bool Returns true if the backup was successful, false otherwise.
     */
    public function backupTable(string $table): bool
    {
        // Create the backup database if it does not exist
        $this->createDatabaseIfNotExists($this->snowflakeBackupDatabase);
        // Create the backup schema if it does not exist
        $this->createSchemaIfNotExists($this->snowflakeBackupSchema);

        $this->backupTableName = $table . "_backup_" . time();

        // $query = "CREATE TABLE {$this->backupTableName} AS SELECT * FROM {$table}";
        // $query = "ALTER TABLE IF EXISTS $table RENAME TO {$this->backupTableName}";

        // Create a query to backup the table into the backup schema
        // within the backup database with the $this->backupTableName
        $query = "CREATE TABLE "
            . (
                $this->snowflakeBackupDatabase . "." . $this->snowflakeBackupSchema
                . "." . $this->backupTableName
            ) . " AS SELECT * FROM "
            . (
                $this->snowflakeDatabase . "." . $this->snowflakeSchema
                . "." . $table
            );

        $this->executeQuery($query);
        
        // check if there were any errors, and return true or false
        return (empty($this->errorInfo) && empty($this->errorMessage));
    }

    /**
     * Archives the sync table by renaming it with a timestamp suffix.
     * The new table name is returned upon success. Otherwise, an empty string is returned.
     *
     * @return string
     */
    public function archiveSyncTable(): string
    {
        $newTableName = '';
        if ($this->isArchiveSyncTableSet() && !empty($this->syncTableName)) {
            $currentTime = time();
            $newTableName = $this->syncTableName . "_" . $currentTime;
            $this->executeQuery(
                "ALTER TABLE IF EXISTS {$this->syncTableName} RENAME TO {$newTableName}"
            );
            $newTableName = (empty($this->errorInfo) && empty($this->errorMessage)) ? $newTableName : '';
        }
        return $newTableName;
    }

    /**
     * Returns the name of the backup table.
     *
     * @return string The name of the backup table.
     */
    public function getBackupTableName(): string
    {
        return $this->backupTableName;
    }

    /**
     * Update the table data on Snowflake, based on incoming JSON data.
     * 
     * Since Snowflake doesn't support INSERT OR UPDATE / INSERT ... ON DUPLICATE KEY UPDATE, we're going to take a multi-step approach: 
     *   1. Based on the received rawData, extract columns and their data types for Snowflake
     *   2. Create the target table on Snowflake if it doesn't exist
     *   3. Fetch Columns from Snowflake from the actual target table
     *   4. Create a temporary table on Snowflake based on the schema of actual target table. (Generate table name with timestamp)
     *   5. Copy/Insert data into the Snowflake Temporary Table
     *   6. Run Merge command with temporary table as source, and data being merged into the target table
     *   7. Remove the temporary table once the Merge completes
     *
     * @param string $rawData The raw JSON data to be upserted in the Snowflake table
     * @param string $table The table into which the data is to be inserted
     * @param string|null $primaryKeyColumn The primary key of the table, based on which the join is to be implemented ahead
     * @param bool $insertWithoutMerge Whether to insert the data directly into the target table without the MERGE command
     * @return int The number of rows affected on Snowflake by this method.
     */
    public function updateTable(string $rawData, string $table, string $primaryKeyColumn = '', bool $insertWithoutMerge = false): int
    {
        try {
            $start = microtime(true);
            $this->verboseLog(PHP_EOL . "======================================" . PHP_EOL . 
                "Snowflake - updateTable Process for `{$table}` - BEGIN" . PHP_EOL);

            $this->rawData = $rawData;
            $isNewTableHandling = (empty($this->lastProcessedTable) || $this->lastProcessedTable !== $table) ;

            // Only attempt table creation, and primary Key column extration if this is an insertion into a new table. 
            if ($isNewTableHandling) {
                $this->cleanTableColumns();
                $this->table = $table;
                $this->primaryKeyColumn = $primaryKeyColumn;
            }
            $this->generateTemporaryTableName();
            
            $jsonData = json_decode($rawData,true);
            
            if ($isNewTableHandling) {
                // Following same logic as in RDS_load
                if (!isset($table)) {
                    foreach ($jsonData[0] as $key => $value){
                        $table = $key;
                    }
                    $jsonData = $jsonData[$table];
                }
            
                // $clist = array_keys($jsonData[0]); //fetch all the keys (columns) from first row of $jsonData
                // Extract the columns and their respecitve data types from the parsed array (of received json data)
                $this->extractTableColumnsAndDataTypes($jsonData[0]);
                
                // Create the Main Target table if it does not exist (essential for the first run)
                $this->createTargetTableIfNotExists($primaryKeyColumn);

                // Get the valid columns that exist in the target table on Snowflake (additional/extra data/fields will be dropped)
                /* 
                We're doing this as it was noticed thatdata import was inconsistent. 
                Data against same table was observed to have less columns, 
                which caused temporary table to  be created with less columns, resulting in an 'invalid identifier' error.
                */ 
                $this->getTargetTableColumns();
            }

            // Is this supposed to be a direct insert into the target table without the MERGE command?
            if ($insertWithoutMerge) {
                $this->loadDataToTable($jsonData, $this->table);
                $affectedRows = ($this->affectedRows > 0) ? $this->affectedRows : 0;
            } else {
                // Create a temporary table to load the received data, which will later be used for the MERGE command
                $this->createTemporaryTable();

                // Load the data into the Temporary Table
                $this->loadDataToTable($jsonData, $this->getTemporaryTableName());

                // Merge the data into the actual Target Table
                $this->mergeDataIntoTargetTable($primaryKeyColumn); 
                $affectedRows = ($this->affectedRows > 0) ? $this->affectedRows : 0;

                // Drop the temporary table
                $this->dropTemporaryTable();
            }
            
            $logMessage = "Successfully inserted data into snowflake table.
Destination Table: " . $this->table . "
Incoming Records: " . count($jsonData) . "
Affected Rows: " . $affectedRows . "
";


            // if ($this->isDebugLoggingEnabled()) {
            //     $this->log('', true, $logMessage, self::$LOGTYPES['TASK_SUMMARY']);
            // }
            $this->verboseLog("Snowflake - updateTable Process for table {$table} - END (" . number_format((microtime(true) - $start), 4) . "s)" . 
                PHP_EOL . "======================================" . PHP_EOL . PHP_EOL);
            // $this->verboseLog($logMessage);
            $this->lastProcessedTable = $table;
            return $affectedRows;
        } catch (\Throwable $th) {
            $this->log('', false, "Exception encountered in " . __FUNCTION__ ." method. " .$th->getMessage());
            throw $th;
        }
        
    }

    /**
     * Method to load data into the temporary table, as it will serve as the source for the Snowflake merge query 
     *
     * @param array $data The data to be inserted/loaded into the table
     * @param string $tableName The name of the table into which the record is to be inserted
     * @return void
     */
    public function loadDataToTable(array $data, string $tableName = ''): void
    {
        // $start = microtime(true);
        $tableName = $tableName ?? $this->getTemporaryTableName();
        // $this->verboseLog("Load Data to `" . $tableName . "` Table - BEGIN");

        $insertColumns = $this->columns;
        if ($this->tableKeyIsAutoIncrement) {
            $insertColumns = array_diff($this->columns, [$this->primaryKeyColumn]);
        }
        // $tableColumns = (count($this->columns) > 0) ? "(" . implode(",", $this->columns). ")" :"";
        $tableColumns = (count($this->columns) > 0) ? "(" . implode(",", $insertColumns). ")" :"";
        
        $sqlTemplate = "INSERT INTO " . $tableName . " " . $tableColumns . " VALUES ";
        $paramsForBinding = [];
        $placeholdersForParams = "";

        $chunkCount = 0;
        $preparedQueryStatements = [];
        $preparedQueryParameters = [];

        $rowsCount = 0;
        $totalRecords = 0; // Total records to be inserted
        foreach ($data as $row ) {
            $tableColumns = array_map("strtoupper", $this->columns);
            //During insertion of bulk data empty columns will be filled with empty string/placholder
            $record = [];
            foreach($row as $key => $value) {
                $record[strtoupper($key)] = $value;
            }

            // Loop through actual column keys
            $primaryKeyFound = false;
            $placeholdersForParams .= "(";
            foreach($this->columns as $column) {
                $value = $record[strtoupper($column)] ?? ""; //if value doesn't exist, substitute with empty value
                $sanitizedString = preg_replace('/\x{FEFF}/u', '', $value);
                if (strtoupper($column) == strtoupper($this->primaryKeyColumn)) {
                    if (!$this->tableKeyIsAutoIncrement) {
                    $placeholdersForParams .= "?,";
                    $paramsForBinding[] = $sanitizedString;
                    }
                    $primaryKeyFound = true;
                } else {
                $placeholdersForParams .= "?,";
                $paramsForBinding[] = $sanitizedString;
                }
            }

            // Add the missing parameter at the end of the values pair
            if (!$primaryKeyFound && $this->tableKeyIsAutoIncrement) {
                // if the the primary key column is in the '$insertColumns' array, then add a placeholder for it
                if (in_array($this->primaryKeyColumn, $insertColumns)) {
                    $placeholdersForParams .= "?";
                    $paramsForBinding[] = 0;
                }
            }

            //strip the trailing comma after the last placeholder & close the parenthesis
            if (strlen($placeholdersForParams) > 0) {
                $placeholdersForParams = rtrim($placeholdersForParams, ",") . "),";
            }

            $rowsCount++;
            if ($rowsCount == $this->getInsertChunkSize()) {
                $placeholdersForParams = rtrim($placeholdersForParams, ",");
                $query = $sqlTemplate . " " . $placeholdersForParams;
                $totalRecords += $rowsCount;
                $preparedQueryStatements[$chunkCount] = $this->conn->prepare($query);
                $preparedQueryParameters[$chunkCount] = $paramsForBinding;

                // For statistics, record the size of the chunk string - Begin
                $chunkStringSize = strlen("('" . implode("','", $paramsForBinding) . "')") ?? 0;
                $this->addChunkLengths($chunkStringSize);
                $this->addChunkRowCount($rowsCount);
                $chunkStringSize = 0;
                // For statistics, record the size of the chunk string - End
    
                $placeholdersForParams = "";
                $paramsForBinding = [];
                $chunkCount++;
                $rowsCount = 0;
            }
        }
        
        // Are there still some records that were being prepared as a chunk? 
        if (!empty($paramsForBinding) && !empty($placeholdersForParams)) {
            $placeholdersForParams = rtrim($placeholdersForParams, ",");
            $query = $sqlTemplate . " " . $placeholdersForParams;
            $totalRecords += $rowsCount;
            $preparedQueryStatements[$chunkCount] = $this->conn->prepare($query);
            $preparedQueryParameters[$chunkCount] = $paramsForBinding;
            
            // For statistics, record the size of the chunk string - Begin
            $chunkStringSize = strlen("('" . implode("','", $paramsForBinding) . "')") ?? 0;
            $this->addChunkLengths($chunkStringSize);
            $this->addChunkRowCount($rowsCount);
            $chunkStringSize = 0;
            // For statistics, record the size of the chunk string - End
            
            $placeholdersForParams = "";
            $paramsForBinding = [];
            $chunkCount++;
            $rowsCount = 0;
        }

        // Now that all queries have been prepared, let's execute them with their respective parameter sets
        $processedRecords = 0;
        for ($i = 0; $i < count($preparedQueryStatements); $i++) {
            try {
                /* foreach($preparedQueryParameters[$i] as $index => $value) {
                    $preparedQueryStatements[$i]->bindParam(($index + 1), $value, $this->getPDODataType($value) );    
                }
                $preparedQueryStatements[$i]->execute(); */
                $preparedQueryStatements[$i]->execute($preparedQueryParameters[$i]);
                $processedRecords = (($i+1) * $this->getInsertChunkSize());
                $processedRecords = ($processedRecords > $totalRecords) ? $totalRecords : $processedRecords;
                // $progress = ($processedRecords / $totalRecords) * 100;
                
                // $logMessage = "Successfully inserted record " . $processedRecords . "/" . $totalRecords .
                //     " (" . number_format($progress, 2) . "%) into `" . $this->getTemporaryTableName() . "`.";
                
                // if ($this->isDebugLoggingEnabled()) {
                //     $this->log('', true, $logMessage, self::$LOGTYPES['TASK_SUMMARY']);
                // }
                // $this->verboseLog($logMessage, true);
            } catch (\PDOException $th) {
                //throw $th;
                // Log the failed query and error message
                $failedQuery = $preparedQueryStatements[$i]->queryString;
                $exceptionMessage = $th->getMessage();

                $errorInfo = $preparedQueryStatements[$i]->errorInfo();
                $errorMessage = isset($errorInfo[2]) ? $errorInfo[2] : 'Unknown error';

                $errorDetails = [
                    'failedQuery' => $failedQuery,
                    'exceptionMessage' => $exceptionMessage,
                    'errorInfo' => $errorInfo,
                    'errorMessage' => $errorMessage,
                ];

                $this->log($query, false, json_encode($errorDetails));
            }
        }

        // $this->verboseLog("Load Data to  `" . $tableName . "` Table - END (" . number_format((microtime(true) - $start), 4) . "s)" . PHP_EOL);
    }

    /**
     * Merge Data into the main table from the source table, based on the primaryKeyColumn join
     *
     * @param string|null $primaryKeyColumn The column that will serve as the key for join between the main table and the temporary table
     * @return void
     */
    public function mergeDataIntoTargetTable(string $primaryKeyColumn = null): void
    {   
        try {
            // $start = microtime(true);

            // $this->verboseLog("Merge Data into table " . $this->table . " - BEGIN");

            $primaryKeyColumn = $primaryKeyColumn ?? 'ID'; //Set this to the primary key column for the table for respective match
            $sourceAlias = "s"; // Source Table Alias
            $targetAlias = "t"; // Target Table Alias
    
            $matchedUpdatePairs = [];
            foreach ($this->columns as $column) { 
                $matchedUpdatePairs[] = "{$targetAlias}.{$column} = {$sourceAlias}.{$column}";
            }
            //Aliasing - 't' for target table, 's' for source table
            $sql = "MERGE INTO " . $this->table . " AS t
            USING ". $this->getTemporaryTableName() . " AS s
            ON t.{$primaryKeyColumn} = s.{$primaryKeyColumn}
            WHEN MATCHED THEN
                UPDATE SET
                    " . implode(", ", $matchedUpdatePairs) ." 
            WHEN NOT MATCHED THEN 
                INSERT (" . implode(", ", $this->columns) . ")
                VALUES ({$sourceAlias}." . implode(", {$sourceAlias}.",  $this->columns) . ")
            ";
            // $this->verboseLog(PHP_EOL . $sql . PHP_EOL);
    
            $this->executeQuery($sql);

            // $this->verboseLog("Merge Data into table " . $this->table . " - END (" . number_format((microtime(true) - $start), 4) . "s)" . PHP_EOL);
        } catch (\Throwable $th) {
            throw $th;
        }
        
    }


    /**
     * This method extracts the table columns and their respective data types from the from the received data array
     *
     * @param array $data This is the data array to be upserted into the table.
     * @return void
     */
    public function extractTableColumnsAndDataTypes(array $data): void
    {
        // $start = microtime(true);
        // $this->verboseLog("Extracting columns from incoming data - BEGIN");

        foreach($data as $columnKey => $value){
            // $this->columns[] = $columnKey;
            $this->columnDataTypes[strtoupper($columnKey)] = $this->getSnowflakeDataType($value); 
            array_push($this->columns, strtoupper($columnKey));
        }

        // Get list of all fields from the entire $this->rawData object
		$incomingColumns = [];
		$rawData = json_decode($this->rawData, true);
		foreach ($rawData as $record) {
			$incomingColumns = array_merge($incomingColumns, array_keys($record));
            // FileMaker maps all records and returns them just like mysql does (even if they are empty),
            // so iterating through the first record is enough to get all the columns
            break;
		}
        $incomingColumns = array_map("strtoupper", $incomingColumns);
		$incomingColumns = array_unique($incomingColumns);

		// check if the incoming columns are different from the existing columns
		// if they are, then we need to update the table
		$diff = array_diff($incomingColumns, array_map("strtoupper", array_keys($this->columns)));
		if (!empty($diff)) {
			// instead of adding the new columns as TEXT directly, let's search for the data type of the new columns
			// let's find the first record that has the new column and get the data type
			foreach ($diff as $newColumn) {
				$firstRecordWithNewColumn = null;
				foreach ($rawData as $record) {
					if (array_key_exists($newColumn, $record)) {
						$firstRecordWithNewColumn = $record;
						break;
					}
				}
                $newColumn = strtoupper($newColumn);
                $this->columns[] = $newColumn;
				if ($firstRecordWithNewColumn) {
					$this->columnDataTypes[$newColumn] = $this->getSnowflakeDataType($firstRecordWithNewColumn[$newColumn]);
				} else {
					// If the new column is not found in any of the records, default to TEXT
					$this->columnDataTypes[$newColumn] = 'TEXT';
				}
			}
		}

        // $this->verboseLog("Extracting columns from incoming data - END (" . number_format((microtime(true) - $start), 4) . "s)" . PHP_EOL);
    }

    /**
     * This method generates the query for creating the main table if it doesn't already exist.
     *
     * @param string|null $primaryKeyColumn The primary key of the table
     * @return void
     */
    public function generateCreateTableIfNotExistsQuery(string $primaryKeyColumn = null): void
    {
        $this->clearCreateTableQuery();
        $this->createTableQuery = "CREATE TABLE IF NOT EXISTS " . $this->table . " (";

        // Make sure no column has been repeated twice, this will mess up the create table query
        $this->columns = array_map("strtoupper", $this->columns);
        $this->columns = array_unique($this->columns);
        $primaryKeyFound = false;
        foreach($this->columns as $column) {
            if (!empty($primaryKeyColumn) && (strtoupper($column) == strtoupper($primaryKeyColumn))) {
                $primaryKeyFound = true;
            } 
            $this->createTableQuery .= " ". $column ." " . $this->columnDataTypes[$column] . " " . (
                (!empty($primaryKeyColumn) && (strtoupper($column) == strtoupper($primaryKeyColumn))) ? 
                    " CONSTRAINT {$column}_primary_key PRIMARY KEY ":""
            ) . ",";
        }
        if (!$primaryKeyFound && !empty($primaryKeyColumn)) {
            $this->createTableQuery .= " ". strtoupper($primaryKeyColumn) ." INTEGER CONSTRAINT {$primaryKeyColumn}_primary_key PRIMARY KEY AUTOINCREMENT "; 
        }
        // Trim the trailing comma after the last field
        $this->createTableQuery = rtrim($this->createTableQuery, ",");
        $this->createTableQuery .= ")";
        $this->createTableQuery; 
    }

    /**
     * This method generates the query for creating the temporary table on Snowflake.
     * It generates the query based on the query for the main table.
     *
     * @return void
     */
    public function generateTemporaryTableQuery(): void
    {
        // A Quick Hack to create the temporary table based on the actual target table if it has been created.
        $this->temporaryTableQuery = "CREATE TEMPORARY TABLE " . $this->getTemporaryTableName()
            . " LIKE " .  $this->table;
    }

    /**
     * Create the target table, if it doesn't already exist
     *
     * @param string|null $primaryKeyColumn The primary key of the table
     * @return void
     */
    public function createTargetTableIfNotExists(string $primaryKeyColumn = null): void
    {
        try {
            // $start = microtime(true);
            $this->generateCreateTableIfNotExistsQuery($primaryKeyColumn);

            // $this->verboseLog("Create Table '" . $this->table . "' if not exists - BEGIN");

            $this->executeQuery($this->getCreateTableQuery());
            $this->tableCreationStatus = empty($this->errorInfo);
            // $this->verboseLog(PHP_EOL . $this->getCreateTableQuery() . PHP_EOL);
            // $this->verboseLog("Create Table '" . $this->table . "' if not exists - END (" . number_format((microtime(true) - $start), 4) . "s)" . PHP_EOL);
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    
    /**
     * This table flushes the existing columns property against the target table
     * and repopulates them based on the description of the table from Snowflake.
     * We haven't used a select query, as the target table will be empty on first creation.
     *
     * @return void
     */
    public function getTargetTableColumns(): void
    {
        try {
            // $start = microtime(true);
            // $this->verboseLog("Get Table Columns from Snowflake for '" . $this->table . "' - BEGIN" . PHP_EOL);

            $query = 'DESCRIBE TABLE ' . $this->table;

            //Fetch one row to get the column order
            $result = $this->executeQuery($query);
            if (!empty($result)) {
                //flush the previously stored columns reference, we're getting them from the target table on Snowflake
                // $this->cleanTableColumns();

                $existingTableColumns = [];
                foreach($result as $fieldInfo) {
                    // $this->columns[] = $fieldInfo['name'];
                    $existingTableColumns[] = $fieldInfo['name'];
                    if ( !empty($fieldInfo['primary key'] && strtoupper($fieldInfo['primary key']) == 'Y')) {
                        $this->primaryKeyColumn = $fieldInfo['name'];
                    }
                    if (
                        !empty($fieldInfo['default']) 
                        && stripos(strtoupper($fieldInfo['default']),'IDENTITY') !== false
                    ) {
                        $this->tableKeyIsAutoIncrement = true;
                    }
                }
            }

            // Compare the columns from the incoming data with the columns from the target table,
            // and update the target table schema if necessary
            $this->updateTargetTableColumns($existingTableColumns);

            // $this->verboseLog(print_r($this->columns, true));
            // $this->verboseLog(PHP_EOL . "Get Table Columns from Snowflake for '" . $this->table . "' - END (" .
            //     number_format((microtime(true) - $start), 4) . "s)" . PHP_EOL);
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    /**
	 * Update the columns (schema) of the target table if the incoming data has new columns.
	 * This method assumes that $this->columns has already been populated with the columns from the incoming data, and will contain any new records
	 *
	 * @param array $existingTableColumns The existing columns of the target table.
	 * @return void
	 */
	public function updateTargetTableColumns(array $existingTableColumns): void
    {
        $alterQuery = "";
        try {
            // $start = microtime(true);
            // $this->verboseLog("Update Target Table Columns - BEGIN");

            // Get the columns that are in the incoming data but not in the target table
            $diff = array_diff(array_values($this->columns), array_values($existingTableColumns));

            if (!empty($diff)) {
                // $this->columns already contained the new columns along with their data types, so we can use it to update the target table schema
                // Create an Alter query to add the new columns to the target table, allowing new columns to be NULL
                $alterQuery .= "ALTER TABLE {$this->table} ADD COLUMN ";
                $alterQuery .= implode(", ", array_map(function ($columnName) {
                    return " $columnName " . $this->columnDataTypes[$columnName] . " NULL ";
                }, $diff));

                // Execute the alter query
                $this->executeQuery($alterQuery);
                // log status of alter query
            }

            // $this->verboseLog("Update Target Table Columns - END (" . number_format((microtime(true) - $start), 4) . " s)");
        } catch (\Throwable $th) {
            // log alter query error, along with the attempted alter query
            $this->log($alterQuery, false,
                "Error encountered while attempting to alter the target table (" . $this->table . "). "
                . $th->getMessage()
            );
            throw $th;
        }
    }

    /**
     * Method to create the temporary table on Snowflake
     *
     * @return void
     */
    public function createTemporaryTable(): void
    {
        try {
            // $start = microtime(true);
            // $this->verboseLog("Create Temporary Table '" . $this->getTemporaryTableName() . "' - BEGIN");

            $this->generateTemporaryTableQuery();
            $this->executeQuery($this->getTemporaryTableQuery());

            // $this->verboseLog("Create Temporary Table '" . $this->getTemporaryTableName() . "' - END (" . number_format((microtime(true) - $start), 4) . "s)" . PHP_EOL);
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    /**
     * Drop the temporary table from Snowflake
     *
     * @return void
     */
    public function dropTemporaryTable(): void
    {
        try {
            $query = "DROP TABLE ". $this->getTemporaryTableName() ;
            $this->executeQuery($query);
        } catch (\Throwable $th) {
            throw $th;
        }
    }

    /**
     * Method to get the syncTable name property
     * 
     * @return string
     */
    public function getSyncTableName(): string
    {
        return $this->syncTableName;
    }

    /**
     * Get the last synced timestamp for a specific table in 'm/d/Y H:i:s' format to mock the behavior of the FileMaker API.
     *
     * @param string $table The name of the table.
     * @param bool $subtractDay Whether to subtract a day from the last synced timestamp.
     * @return string|null The last synced timestamp, or null if no record was found.
     */
    public function getLastSyncedTimestamp($table, $subtractDay = true)
    {
        // Make sure the sync_status table exists, and create it if it doesn't
        if (!$this->ensureSyncStatusTableExists()) {
			return null;
		}
        // Prepare the SQL query
        $sql = "SELECT date_modified FROM " . $this->getSyncTableName()
            . " WHERE data_table = ? ORDER BY date_time DESC LIMIT 1";
    
        // Execute the query and fetch the result
        $stmt = $this->conn->prepare($sql);
        $stmt->execute([$table]);
        $result = ($stmt->rowCount() > 0) ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];

        $lastSyncedTimestamp = null;
        if (isset($result) && isset($result[0]['DATE_MODIFIED'])) {
			// Convert the timestamp to a string in 'm/d/Y H:i:s' format, and take the date one day back
			// $lastSyncedTimestamp = date('m/d/Y H:i:s', strtotime($result[0]['DATE_MODIFIED']));
            $lastSyncedTimestamp = $result[0]['DATE_MODIFIED'] ?? $lastSyncedTimestamp;
            $timestampForDate = ($subtractDay)
				? strtotime($lastSyncedTimestamp . ' -1 day')
				: strtotime($lastSyncedTimestamp);
			$lastSyncedTimestamp = date('m/d/Y 00:00:00', $timestampForDate);
			// $lastSyncedTimestamp = date('m/d/Y 00:00:00', strtotime($result[0]['DATE_MODIFIED'] . ' -1 day'));
		}

        // Return the last synced timestamp, or null if no record was found
        return $lastSyncedTimestamp;
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
        $sql = "SHOW TABLES LIKE '" . $this->getSyncTableName() . "'";

        // Execute the query
        $result = $this->executeQuery($sql);

        // If the sync_status table doesn't exist, create it
        if (empty($result) || count($result) == 0) {
            // $sql = "
            //     CREATE TABLE " . $this->getSyncTableName() . " (
            //         table_name VARCHAR(255) NOT NULL,
            //         last_synced_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            //         PRIMARY KEY (table_name)
            //     )
            // ";

            // // Execute the query
            // $this->executeQuery($sql);

			// We're not creating the table now, as it will be created when the first log record is inserted
            return false;
        }
        return true;
    }

    /**
     * Update the last synced timestamp for a specific table.
     *
     * @param string $table The name of the table.
     * @param string $timestamp The timestamp to be set.
     * @return void
     */
    public function updateLastSyncedTimestamp($table, $timestamp)
    {
        // Make sure the sync_status table exists, and create it if it doesn't
        $this->ensureSyncStatusTableExists();

        // convert timestamp to mysql acceptable timestamp (Y-m-d H:i:s)
        $timestamp = date('Y-m-d H:i:s', strtotime($timestamp));

        // Prepare the SQL query
        // $sql = "INSERT INTO "  . $this->getSyncTableName() . " (table_name, last_synced_timestamp) VALUES (?, ?)
        //     ON DUPLICATE KEY UPDATE last_synced_timestamp = ?";
        //Replace with merge command for Snowflake
        $sql = "MERGE INTO " . $this->getSyncTableName() . " AS t
            USING (SELECT ? AS table_name, ? AS last_synced_timestamp) AS s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN
                UPDATE SET
                    t.last_synced_timestamp = s.last_synced_timestamp
            WHEN NOT MATCHED THEN 
                INSERT (table_name, last_synced_timestamp)
                VALUES (s.table_name, s.last_synced_timestamp)
            ";
    
        // Execute the query
        $stmt = $this->conn->prepare($sql);
        // $stmt->execute([$table, $timestamp, $timestamp]);
        $stmt->execute([$table, $timestamp]);
    }

    /**
     * Initialize the sync table
     *
     * @return void
     */
    public function initializeSyncTable()
    {
        // Make sure the sync_status table exists, and create it if it doesn't
        $this->ensureSyncStatusTableExists();

        // Prepare the SQL query
        $sql = "TRUNCATE TABLE " . $this->getSyncTableName();

        // Execute the query
        $this->executeQuery($sql);
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
