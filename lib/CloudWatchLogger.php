<?php
require_once './vendor/autoload.php';
require_once './config.php';
use Aws\CloudWatchLogs\CloudWatchLogsClient;

class CloudWatchLogger
{
    private $client;
    private $logGroupName;
    private $logStreamName;

    public function __construct($logGroupName, $logStreamName)
    {
        global $config;
        $this->client = new CloudWatchLogsClient([
            'region' => $config['aws']['region'],
            'version' => $config['aws']['version'],
            'credentials' => [
                'key' => $config['aws']['key'],
                'secret' => $config['aws']['secret']
            ]
        ]);

        $this->logGroupName = $logGroupName;
        $this->logStreamName = $logStreamName;
    }

    public function log($message)
    {
        $this->createLogGroup();
        $this->createLogStream();
        $this->createLogEvent($message);
    }

    private function createLogGroup()
    {
        try {
            $this->client->createLogGroup([
                'logGroupName' => $this->logGroupName
            ]);
        } catch (\Exception $e) {
            // The log group already exists, do nothing
        }
    }

    private function createLogStream()
    {
        try {
            $this->client->createLogStream([
                'logGroupName' => $this->logGroupName,
                'logStreamName' => $this->logStreamName
            ]);
        } catch (\Exception $e) {
            // The log stream already exists, do nothing
        }
    }

    private function createLogEvent($message)
    {
        try {
            $this->client->createLogStream([
                'logGroupName' => $this->logGroupName,
                'logStreamName' => $this->logStreamName
            ]);
        } catch (\Exception $e) {
            $this->client->putLogEvents([
                'logGroupName' => $this->logGroupName,
                'logStreamName' => $this->logStreamName,
                'logEvents' => [
                    [
                        'timestamp' => time() * 1000,
                        'message' => $message
                    ]
                ]
            ]);
        }
    }
}
