<?php

require __DIR__ . '/vendor/autoload.php';
use Aws\Athena\AthenaClient;

class connectAthena
{

    public $options;
    public $table;
    public $databaseName;
    public $catalog;
    public $sql;
    public $athenaClient;
    public $outputS3Location;


    public function __construct($table,$sql){
        $this->options = [
            'version' => 'latest',
            'region' => 'us-west-2',
            'credentials' => [
                'key' => 'AKIAWSWWCDIKZNE4K6UH',
                'secret' => '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'
            ]];
        $this->table = $table;
        $this->sql = $sql;
        $this->catalog = "AwsDataCatalog";
        $this->databaseName = "athenadb";
        $this->outputS3Location = 's3://dme-athena-queries/staging/';

    }

    function __destruct() {
        //print "Destroying " . __CLASS__ . "\n";
    }

    function executeQuery(){
        $options = $this->options;
        $this->athenaClient = new AthenaClient($options);

        $athenaClient = $this->athenaClient;

        $startQueryResponse = $this->athenaClient->startQueryExecution([
            'QueryExecutionContext' => [
                'Catalog' => $this->catalog,
                'Database' => $this->databaseName
            ],
            'QueryString' => $this->sql,
            'ResultConfiguration' => [
                'OutputLocation' => $this->outputS3Location
            ]
        ]);

        $queryExecutionId = $startQueryResponse->get('QueryExecutionId');

        $waitForSucceeded = function () use ($athenaClient, $queryExecutionId, &$waitForSucceeded) {
            $getQueryExecutionResponse = $athenaClient->getQueryExecution([
                'QueryExecutionId' => $queryExecutionId
            ]);
            $status = $getQueryExecutionResponse->get('QueryExecution')['Status']['State'];

            return ($status === 'SUCCEEDED' || $status === "FAILED") || $waitForSucceeded();
        };

        $waitForSucceeded();

        return $athenaClient->getQueryResults([
            'QueryExecutionId' => $queryExecutionId
        ]);
    }
}