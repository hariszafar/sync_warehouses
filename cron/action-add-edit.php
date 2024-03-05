<?php
require __DIR__ . DIRECTORY_SEPARATOR. '../vendor/autoload.php';

use TiBeN\CrontabManager\CrontabAdapter;
use TiBeN\CrontabManager\CrontabRepository;
use TiBeN\CrontabManager\CrontabJob;

try {
    /* Capture the incoming parameters for creating/editing the cron job, based on this form */
    $oldCronRule = $_POST['oldCronRule'] ?? null;
    $cronMinute = $_POST['cronMinute'] ?? '*';
    $cronHour = $_POST['cronHour'] ?? '*';
    $cronDayOfMonth = $_POST['cronDayOfMonth'] ?? '*';
    $cronMonth = $_POST['cronMonth'] ?? '*';
    $cronDayOfWeek = $_POST['cronDayOfWeek'] ?? '*';
    $cronCommand = $_POST['cronCommand'] ?? null;
    $description = $_POST['description'] ?? '-';
    $enabled = (isset($_POST['enabled']) && $_POST['enabled']) ? true : false;

    $existingJob = false;

    if (empty($cronCommand)) {
        throw new \Exception("Cron command cannot be empty");
    }

    $crontabRepository = new CrontabRepository(new CrontabAdapter());

    // Was an old cron job being edited?
    if (!empty($oldCronRule)) {
        $existingJob = true;
        $oldCronObject = json_decode($oldCronRule, true);
        $allCrontabJobs = $crontabRepository->getJobs();
        // Create a cron job object from the old cron rule, to be used for searching
        $oldCronJob = new CrontabJob();
        $oldCronJob
            ->setEnabled($oldCronObject['enabled'])
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
            // Get the old cron job and update it with the new values
            $crontabJob = $allCrontabJobs[$result];
            $crontabJob
                ->setEnabled($enabled)
                ->setMinutes($cronMinute)
                ->setHours($cronHour)
                ->setDayOfMonth($cronDayOfMonth)
                ->setMonths($cronMonth)
                ->setDayOfWeek($cronDayOfWeek)
                ->setTaskCommandLine($cronCommand)
                ->setComments($description);
        } else {
            throw new \Exception("Failed to edit job. Referenced cron job not found.");
        }
    } else {
        // Create the new cron job object
        $crontabJob = new CrontabJob();
        $crontabJob
            ->setEnabled($enabled)
            ->setMinutes($cronMinute)
            ->setHours($cronHour)
            ->setDayOfMonth($cronDayOfMonth)
            ->setMonths($cronMonth)
            ->setDayOfWeek($cronDayOfWeek)
            ->setTaskCommandLine($cronCommand)
            ->setComments($description); // Comments are persisted in the crontab
        $crontabRepository->addJob($crontabJob);
    }

    // Finally, save the changes to the crontab
    $crontabRepository->persist();

    // Return a JSON response with the success message, the status code 200,
    // and the new cron job
    http_response_code(200);
    echo json_encode([
        'status' => 'success',
        'message' => ($existingJob) ? 'Cron job edited successfully' : 'New Cron job added successfully',
        'data' => $crontabJob
    ]);
    
} catch (\Throwable $th) {
    // Return a JSON response with the error message, and the status code 400
    http_response_code(400);
    echo json_encode([
        'status' => 'error',
        'message' => $th->getMessage()
    ]);
}