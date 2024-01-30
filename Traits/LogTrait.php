<?php

// $called = !empty($called) ? $called++ : 1;
// echo "Called: {$called}" . PHP_EOL;
trait LogTrait
{
    public $logFilePath = null;
    public $queryLogsEnabled = false;
    
    public $lastQuery = '';
    public $lastQueryParams = [];
    
    public $verboseLogsEnabled = false; //These are the logs generated/shown during script execution
    public static $LOGTYPES = [
        'QUERY' => 'Query Log',
        'TASK_SUMMARY' => 'Task Sumary Log',
    ];
    public $debugLogging = true; // This flag controls whether debug logs will be written to the log file

    /**
     * Set flag for enabling/disabling verbose logging during script execution.
     * If set to true, logs of each portion of the updateTable method will be generated during script execution
     *
     * @param boolean $verbose
     * @return void
     */
    public function setVerboseExecutionLogging(bool $verbose = true): void
    {
        $this->verboseLogsEnabled = $verbose;
    }

    /**
     * Returns whether vebose logs should be generated during script execution
     *
     * @return boolean
     */
    public function isVerboseExecutionLoggingEnabled(): bool
    {
        return $this->verboseLogsEnabled;
    }

    /**
     * Show the verbose logs during script execution
     * 
     * @param string $message The message to be logged
     * @param bool $forceShow Whether to force show the message, even if verbose logging is disabled
     * @return void 
     */
    public function verboseLog(string $message = '', bool $forceShow = false): void
    {
        if ($this->isVerboseExecutionLoggingEnabled() || $forceShow) {
            echo $message . PHP_EOL;
        }
    }

    /**
     * Log the executed query or action and its outcome
     *
     * @param string $query The executed query
     * @param bool $status The status of the executed query
     * @param string $message Any message that has been received in order to be logged
     * @param string $logType The type of log to be generated. If not provided, the default is "Query Log"
     * @return void
     */
    private function log(string $query = '', bool $status = true, string $message = null, string $logType = null): void
    {
        try {
            //code...
            if ($logType === null) {
                $logType = self::$LOGTYPES['QUERY'];
            }
            $isTaskSummaryLog = ($logType === self::$LOGTYPES['TASK_SUMMARY']);
            // We will only be logging if either query logs are enabled OR if there is an error 
            if ($this->queryLogsEnabled || !$status || $isTaskSummaryLog) {
                $errorText = ($status === true) ? "-" : "**ERROR** -";
                $logMessage = "[" . date("Y-m-d H:i:s") . "] {$errorText} "; 
                $logMessage .= (($isTaskSummaryLog) ? self::$LOGTYPES['TASK_SUMMARY'] : self::$LOGTYPES['QUERY']) . ", ";
                $logMessage .= (strlen($query) > 0) ? " Query: {$query}, " : "";
                $logMessage .= "Status: " . (($status === true) ? 'Success' : 'Failure') ;
        
                if ($message !== null) {
                    $logMessage .= ", $errorText Message: {$message}";
                }

                // Create the log file, if it doesn't exist
                $this->createLogFileIfNotExists(); 
    
                // Append the log entry to the file
                file_put_contents($this->logFilePath, $logMessage . PHP_EOL, FILE_APPEND);
            }
        } catch (\Throwable $th) {
            echo "Error in " . __METHOD__ . ": " . $th->getMessage() . PHP_EOL;
        }
    }

    
    /**
     * Creates a log file if it doesn't exist, along with any missing directories in the path.
     * The logFilePath propery of the class is used to determine the path of the log file.
     *
     * @return void
     */
    public function createLogFileIfNotExists(): void
    {
        try {
            // Get the absolute directory path
            // $logFilePath = realpath($this->logFilePath);
            $logFilePath = dirname($this->logFilePath);
    
            // Create directories if they don't exist
            $directory = pathinfo($logFilePath, PATHINFO_DIRNAME);
            if (!is_dir($directory)) {
                mkdir($directory, 0755, true);
            }
    
            // Check if the log file exists
            if (file_exists($this->logFilePath)) {
                // Read the content of the log file
                $currentContent = file_get_contents($this->logFilePath);
    
                // Check if "Log File Initialized" is not found at the beginning
                if (strpos($currentContent, "Log File Initialized") !== 0) {
                    // Clean the entire file and insert the initialization text with timestamp
                    $newContent = "Log File Initialized - [" . date('Y-m-d H:i:s') . "]
\n";
                    file_put_contents($this->logFilePath, $newContent);
                }
            } else {
                // Create the log file if it doesn't exist
                file_put_contents($this->logFilePath, "Log File Initialized - [" . date('Y-m-d H:i:s') . "]
\n");
            }
        } catch (\Throwable $th) {
            echo "Error in " . __METHOD__ . ": " . $th->getMessage() . PHP_EOL;
        }
    }

    
    /**
     * Clear the contents of the log file and re-initialize it with the initialization text and timestamp.
     * Do not append to file, rather overwrite the entire file.
     *
     * @return void
     */
    public function clearLogFile(): void
    {
        try {
            // Create the log file, if it doesn't exist
            $this->createLogFileIfNotExists(); 
    
            // Clear the contents of the log file
            file_put_contents($this->logFilePath, "Log File Initialized - [" . date('Y-m-d H:i:s') . "]");
        } catch (\Throwable $th) {
            echo "Error in " . __METHOD__ . ": " . $th->getMessage() . PHP_EOL;
        }
    }

    /**
     * Check whether debug logging is enabled
     * 
     * @return bool
     */
    public function isDebugLoggingEnabled(): bool
    {
        return $this->debugLogging;
    }

    /**
     * Enable/disable debug logging
     * 
     * @param bool $enabled
     * @return void
     */
    public function setDebugLogging(bool $enabled = true): void
    {
        $this->debugLogging = $enabled;
    }

}