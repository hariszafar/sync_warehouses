<?php

// require '../vendor/autoload.php';
require __DIR__ . DIRECTORY_SEPARATOR. '../vendor/autoload.php';

use TiBeN\CrontabManager\CrontabAdapter;
use TiBeN\CrontabManager\CrontabRepository;


// Fetch all the cron jobs
$output = '';
/* $cronJobs = shell_exec('crontab -l');
// exec('crontab -l', $output, $resultCode);
// $cronJobs = explode("\n", trim($output));   // Split the cron jobs by newline
// exec('crontab -l', $cronJobs, $resultCode);
// echo $cronJobs = shell_exec('/etc/crontab -l');
// $cronJobs = shell_exec('php "echo \'poop\';"');
die(print_r($cronJobs));
echo "<pre>";
echo PHP_EOL;
print_r($cronJobs);
print_r($resultCode);
echo PHP_EOL;
echo "</pre>";
echo PHP_EOL;
die(); */


try {
    // Fetch all the cron jobs using the tiben/crontab-manager package
    $crontabRepository = new CrontabRepository(new CrontabAdapter());
    $cronJobs = $crontabRepository->getJobs();

    /*  Sample output
        Array
        (
            [0] => TiBeN\CrontabManager\CrontabJob Object
                (
                    [enabled] => 1
                    [minutes] => 0
                    [hours] => 1
                    [dayOfMonth] => *
                    [months] => *
                    [dayOfWeek] => 1
                    [taskCommandLine] => php echo "Hello world";
                    [comments] =>
                    [shortCut] =>
                )

            [1] => TiBeN\CrontabManager\CrontabJob Object
                (
                    [enabled] => 1
                    [minutes] => 0
                    [hours] => 1
                    [dayOfMonth] => *
                    [months] => *
                    [dayOfWeek] => 2
                    [taskCommandLine] => php echo "2nd Hello world";
                    [comments] =>
                    [shortCut] =>
                )

            [2] => TiBeN\CrontabManager\CrontabJob Object
                (
                    [enabled] =>
                    [minutes] => 0
                    [hours] => 1
                    [dayOfMonth] => *
                    [months] => *
                    [dayOfWeek] => 3
                    [taskCommandLine] => php echo "Job 3";
                    [comments] =>
                    [shortCut] =>
                )

            [3] => TiBeN\CrontabManager\CrontabJob Object
                (
                    [enabled] =>
                    [minutes] => 0
                    [hours] => 1
                    [dayOfMonth] => *
                    [months] => *
                    [dayOfWeek] => 1
                    [taskCommandLine] => php echo "Job 4";
                    [comments] =>
                    [shortCut] =>
                )

            [4] => TiBeN\CrontabManager\CrontabJob Object
                (
                    [enabled] => 1
                    [minutes] => 0
                    [hours] => 1
                    [dayOfMonth] => *
                    [months] => *
                    [dayOfWeek] => 5
                    [taskCommandLine] => php echo "Job 5";
                    [comments] =>
                    [shortCut] =>
                )

        )
     */
    
    $cronJobs = (!empty($cronJobs)) ? $cronJobs : [];

    header('Content-Type: application/json');
    // set the HTTP response code to 200
    http_response_code(200);
    // return the json response
    echo json_encode([
        'status' => 'success',
        'data' => $cronJobs
    ]);
} catch (\Throwable $th) {

    header('Content-Type: application/json');
    // set the HTTP response code to 400
    http_response_code(400);
    echo json_encode([
        'status' => 'error',
        'message' => $th->getMessage()
    ]);
}