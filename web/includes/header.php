<?php
if (!isset($config)) {
    include __DIR__ . '/../../config.php';
}
?>

<!DOCTYPE html>
<html>
<head>
    <title>Cron Manager</title>
    <!-- Add  Bootstrap CDNs -->
    <!-- Bootstrap CSS -->
    <link rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"
        integrity="sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN"
        crossorigin="anonymous"
        async>
    <!-- jQuery CDN -->
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"
        integrity="sha256-/JqT3SQfawRcv/BIHPThkBvs0OEvtFFmqPF/lYI/Cxo="
        crossorigin="anonymous"></script>
</head>
<body>
    <header>
        <nav class="navbar navbar-expand-lg navbar-light bg-light">
            <a  class="navbar-brand" href="#">Periscope Cron Manager</a>
            <div class="collapse navbar-collapse" id="navbarNav">
                <ul class="navbar-nav">
                    <li class="navbar-item">
                        <a class="nav-link" href="/web/cron-manager.php">Cron Manager</a>
                    </li>
                    <!-- <li><a href="/web/index.php">Home</a></li> -->
                </ul>
            </div>
        </nav>
    </header>
    <main>