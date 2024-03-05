<?php 
require __DIR__ . DIRECTORY_SEPARATOR. '../vendor/autoload.php';

use TiBeN\CrontabManager\CrontabAdapter;
use TiBeN\CrontabManager\CrontabRepository;
use TiBeN\CrontabManager\CrontabJob;

try {
    /* Capture the incoming parameters for creating/editing the cron job, based on this form */
    $cronRule = $_POST['cronRule'] ?? null;
    
    if (empty($cronRule)) {
        throw new \Exception("Missing or incorrect cron rule reference");
    }

    $crontabRepository = new CrontabRepository(new CrontabAdapter());
    // Fetch all the rcisting cron jobs
    $allCrontabJobs = $crontabRepository->getJobs();
    
    // Create the new cron job object
    $oldCronObject = json_decode($cronRule, true);
    $oldCronJob = new CrontabJob();
    $oldCronJob
        ->setEnabled(((isset($oldCronObject['enabled']) && $oldCronObject['enabled']) ? true : false))
        ->setMinutes($oldCronObject['minutes'])
        ->setHours($oldCronObject['hours'])
        ->setDayOfMonth($oldCronObject['dayOfMonth'])
        ->setMonths($oldCronObject['months'])
        ->setDayOfWeek($oldCronObject['dayOfWeek'])
        ->setTaskCommandLine($oldCronObject['taskCommandLine'])
        ->setComments($oldCronObject['comments']);
    // Search for the referenced cron job
    $result = array_search($oldCronJob, $allCrontabJobs);
    if ($result !== false) {
        $crontabRepository->removeJob($allCrontabJobs[$result]);
        $crontabRepository->persist();
        http_response_code(200);
        echo json_encode([
            'status' => 'success',
            'message' => 'Cron job deleted successfully'
        ]);
        exit();
    } else {
        throw new \Exception("Failed to delete job. Referenced cron job not found.");
    }
    // Finally, save the changes to the crontab
    $crontabRepository->persist();
    // Return a JSON response with the success message, the status code 200,
    // and the new cron job
    http_response_code(200);
    echo json_encode([
        'status' => 'success',
        'message' => 'Cron job added/edited successfully'
    ]);
    exit();
} catch (\Throwable $th) {
    // Return a JSON response with the error message, and the status code 400
    http_response_code(400);
    echo json_encode([
        'status' => 'error',
        'message' => $th->getMessage()
    ]);
}