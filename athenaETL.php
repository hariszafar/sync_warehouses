<?php

require_once(__DIR__ . DIRECTORY_SEPARATOR . 'vendor' . DIRECTORY_SEPARATOR . 'autoload.php');
require_once(__DIR__ . DIRECTORY_SEPARATOR . 'RDS_load.php');
require_once(__DIR__ . DIRECTORY_SEPARATOR . 'config.php');
require_once(__DIR__ . DIRECTORY_SEPARATOR . 'connectAthena.php');


$rds = new RDS_load($config);

$tables = ["workcalendar", "projectrequests"];

$tablessql = [];

$tablesql["workcalendar"] = 'select  firstname,
    lastname,
    lastdayyouareacceptingwork as lastdayofwork,
    dateyouareacceptingwork as firstdayofwork,
    email,
    whatareyoucapableofcompletingwhileaway as awaywork,
    areyoua as persona,
    entry.adminlink as adminlink,
    entry.dateupdated as date_modified,
    entry.datecreated as date_created,
    id from workcalendar WHERE Date(lastdayyouareacceptingwork) > Now()';

$tablesql["projectrequests"] = 'select  id,
    approved,
    datecompleted as date_completed,
    actiontaken,
    entry.datecreated as date_created,
    entry.editlink as editlink,
    entry.viewlink as viewlink,
    basicinformation.email as email,
    basicinformation.phone as phone,
    basicinformation.projectname as projectname,
    basicinformation.yourname as requestedby,
    basicinformation.neededby as neededby,
    basicinformation.projectsponsor as sponsoredby,
    basicinformation.rationale as rationale,
    basicinformation.projecttype as type,
    basicinformation.projectdescription as description
    from projectrequests';

foreach ($tables as $table) {

    $sql = $tablesql[$table];

    $connection = new connectAthena($table, $sql);
    $getQueryResultsResponse = $connection->executeQuery();

    $cols = $getQueryResultsResponse['ResultSet']['Rows'][0]['Data'];
    $rows = $getQueryResultsResponse['ResultSet']['Rows'];

    $colArr = [];
    foreach ($cols as $k[0]) {
        $colArr[] = ($k[0]["VarCharValue"]);
    }

    $records = [];
    for ($row = 1; $row < count($rows); $row++) {
        //print_r($row);
        $record = [];
        for ($c = 0; $c < count($colArr); $c++) {
            $col = $colArr[$c];
            $record[$col] = array_key_exists('VarCharValue', $rows[$row]["Data"][$c]) ? $rows[$row]["Data"][$c]['VarCharValue'] : "";
        }
        $records[] = $record;
    }

    $rs = json_encode($records, JSON_PRETTY_PRINT);
    //print_r($table . ":" . count($records));
    print_r($rs);

    $dest_table = $table;
    $rds->updateTable($rs, $dest_table);
}



