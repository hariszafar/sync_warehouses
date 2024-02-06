<?php

/**
 * Author: Haris Zafar
 * This file contains the generalised functions used in the ETL process.
 * 
 */

 if (!function_exists('prepareSearch')) {
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
                    $lastSyncedTimestamp = $targetLoader->getLastSyncedTimestamp($destTable) ?? $defaultFirstDateTime;

                    $lastSyncedTimestamp = explode('.', $lastSyncedTimestamp ?? '')[0]; //explode the timestamp and remove the trailing zeros

                    if ($verboseLogLevel > 1) {
                        echo "Timestamps for search values: " . PHP_EOL;
                        echo "lastSyncedTimestamp => " . var_dump($lastSyncedTimestamp) . PHP_EOL;
                        // echo "offsetTimestamp => " . var_dump($offsetTimestamp) . PHP_EOL;
                        echo "Serach before replacing:" . PHP_EOL;
                        var_dump($search);
                    }
                    foreach ($search as $key => $value) {
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
}

if (!function_exists('searchContainsTimestamps')) {
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
}

if (!function_exists('shouldExtractOnce')) {
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
}

if (!function_exists('getTimeSort')) {
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
}

if (!function_exists('rdsEtlLogUpdate')) {
    /**
     * Updates the ETL log in the database with the summary of records modified, added, and unchanged.
     *
     * @param RDS_Load $rds The RDS object used for database operations.
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
}

if (!function_exists('snowEtlLogUpdate')) {
    /**
     * Updates the ETL log with the Snowflake records.
     *
     * @param SnowflakeLoader $snow The Snowflake object.
     * @param array $logRecord The log records containing affected rows for each table.
     * @param int $timeLapse The total time lapse for the particular table.
     * @param string $lastModifiedTimestamp The timestamp of the last modification.
     * @return void
     */
    function snowEtlLogUpdate(&$snow, $logRecord, $timeLapse, $lastModifiedTimestamp)
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
}

if (!function_exists('extractHCPCS')) {
    /**
     * Extract HCPCS codes from a paragraph.
     *
     * @param string $paragraph The paragraph to extract HCPCS codes from.
     * @return string The extracted HCPCS codes separated by commas.
     */
    function extractHCPCS($paragraph)
    {
        $pattern = '/\b[A-Z]{1}[0-9]{4}\b/'; // Pattern to match HCPCS codes
        preg_match_all($pattern, $paragraph, $matches); // Perform regex matching

        $hcpcsArray = $matches[0]; // Extract matched HCPCS codes
        $hcpcsString = implode(',', $hcpcsArray); // Join codes with commas

        return $hcpcsString; // Return HCPCS codes separated by commas
    }
}

if (!function_exists('sumRes')) {
    /**
     * Sum the results of the ETL process.
     *
     * @param array $ur The results of the ETL process.
     * @return array The summed results.
     */
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
}