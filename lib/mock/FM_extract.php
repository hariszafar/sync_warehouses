<?php

class FM_extract
{
    private $host;
    private $username;
    private $password;
    private $database;
    private $connection;
    private $source_table;
    private $dest_table;
    public $search;
    public $limit;
    public $offset;
    public $sort;
    public $column_map;
    public $resultCount = 0;
    public $debug = false;

    public function __construct($db, $host, $source_table, $dest_table, $search, $limit, $offset, $sort, $column_map, $debug = false)
    {
        $this->host = $host;
        $this->username = $db['username'];
        $this->password = $db['password'];
        $this->database = $db['database'];
        $this->source_table = $source_table;
        $this->dest_table = $dest_table;
        $this->search = $search;
        $this->limit = $limit;
        $this->offset = $offset ?? 0;
        $this->sort = $sort;
        $this->column_map = $column_map;
        $this->debug = $debug;
    }

    public function connect()
    {
        $this->connection = new mysqli($this->host, $this->username, $this->password, $this->database);
        if ($this->connection->connect_error) {
            die("Connection failed: " . $this->connection->connect_error);
        }
    }

    public function disconnect()
    {
        $this->connection->close();
    }

    public function fetchDataAsJson($query)
    {
        $result = $this->connection->query($query);
        if (!$result) {
            die("Query failed: " . $this->connection->error);
        }
        $data = [];
        /* while ($row = $result->fetch_assoc()) {
            $data[] = $row;
        } */
        $data_out = [];

        //list of records;
        $data_out['records'] = [];

        // iterate over all records and reformat date, build collections....
        while ($row = $result->fetch_assoc()) {
            $line = [];

            foreach ($row as $col => $value) {
                $value = trim($value);
                $col = (isset($column_map[$col])) ? $column_map[$col] : $col;

                // for columns which are dates add them to a dates collection
                if (explode("_", $col)[0] == "date") {

                    if ($value != "" && strtotime($value) !== false) {

                        $date0 =  date_create($value);

                        $date2 = date_format($date0, 'Y-m-d');
                        $date1 = date_format($date0, 'Y-m-d H:i:s');

                        $date = (strpos($value, ":")) ? $date1 : $date2;

                        $line[$col] = $date;
                    } else {
                        $line[$col] = "";
                    }
                } else {
                    $column = str_replace(" « ", " ", $col);

                    $c =  (isset($column_map[$column])) ? $column_map[$column] : $col;
                    if ($c == "state") {
                        strtoupper($value);
                    }
                    $line[$c] = $value;
                }
            }

            $data_out['records'][] =  $line;
        }

        $result->free();
        $this->resultCount = count($data_out['records']);
        return json_encode($data_out["records"], true);
    }

    public function getRecordSet()
    {
        $host = $this->host;
        $source_table = $this->source_table;
        $dest_table = $this->dest_table;
        $search = $this->search;
        $limit = $this->limit;
        $offset = $this->offset ?? 0;
        $column_map = $this->column_map;

        $norecords = false;

        // $query = [];
        // $query = ['query' => $search];
        // $query['limit'] = $limit;

        // if ($this->debug) {
        //     var_dump($this->search);
        //     die();
        // }

        if (is_array($this->sort) && isset ($this->sort['fieldName'])) {
            $sortOrder = ($this->sort['sortOrder']) ? $this->sort['sortOrder'] : 'ASC';
            if (strtolower($sortOrder) == 'ascend') {
                $sortOrder = 'ASC';
            } else if (strtolower($sortOrder) == 'descend') {
                $sortOrder = 'DESC';
            }
            $sort = ' ORDER BY ' . $this->sort['fieldName'] . ' ' . $sortOrder;
        } else {
            $sort = '';
        }

        // build the $search parameter - the incoming parameter is an array where elements will have column and required values separated by a colon
        // The value before the colon is the column name and the value after the colon is the value to be searched for
        // e.g. $search = ['column1:value1', 'column2:value2', 'column3:value3']
        // only consider search entries containing 'modificationHostTimestamp'. Do not consider other array elements or fields for search
        $search = "";
        $searches = [];
        foreach ($this->search as $key => $value) {
            $actualValue = $value;
            $columnKey = $key;
            while (is_array($actualValue)) {
                foreach ($actualValue as $key => $value) {
                    $actualValue = $value;
                    $columnKey = $key;
                }
            }
            $value = $actualValue;
            $key = $columnKey;

            // if (stripos($value, ":") !== false && stripos($value, "modificationHostTimestamp") !== false) {
            // if (strtolower($key) == strtolower("modificationHostTimestamp")) {
            if (in_array(strtolower($key), [strtolower("modificationHostTimestamp"), strtolower("timestamp")])) {
                // $searchData = explode(":", $value);
                // $column = $searchData[0];
                // $value = $searchData[1];
                $column = $key;

                // if the search string contains a date range (identified by three dots '...' in between), then split the date range into two dates and add the two dates to the search string
                // otherwise add set the date according to the comparison attribute accompanied by the date
                if (stripos($value, "...") !== false) {
                    $dateRange = explode("...", $value);
                    $date1 = $dateRange[0];
                    $date2 = $dateRange[1];

                    $searches[] =  " " . $column  . " BETWEEN '" . trim($date1) . "' AND '" . trim($date2) . "'";
                } else {
                    if (strpos($value, ">=") !== false) {
                        $operator = ">=";
                    } else if (strpos($value, "<=") !== false) {
                        $operator = "<=";
                    } else if (strpos($value, ">") !== false) {
                        $operator = ">";
                    } else if (strpos($value, "<") !== false) {
                        $operator = "<";
                    } else if (strpos($value, "=") !== false) {
                        $operator = "=";
                    } else {
                        $operator = "=";
                    }
                    $value = str_replace($operator, "", $value);
                    $search .= " " . $column . " " . $operator . " '" . $value . "'";
                }
            }

        }
        if (count($searches) > 0) {
            $search = " WHERE " . implode(" AND ", $searches);
        }
        $this->connect();

        // $query = "SELECT * FROM $source_table WHERE $search LIMIT $limit OFFSET $offset";
        $query = "SELECT * FROM {$source_table} {$search} {$sort} LIMIT {$limit} OFFSET {$offset}";
        if ($this->debug) {
            //echo $query;
            echo ($query);
        }
        $data = $this->fetchDataAsJson($query);

        $this->disconnect();

        if ($this->resultCount == 0) {
            $data = json_encode([]);
        }

        return $data;
    }
}
