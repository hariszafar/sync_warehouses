<?php

$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, 'https://172.31.61.179/fmi/api/v1/databases');
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

// Add the API token to the Authorization header.
curl_setopt($ch, CURLOPT_HTTPHEADER, array(
    'Authorization: Bearer YOUR_API_TOKEN',
));

$response = curl_exec($ch);

if (curl_errno($ch)) {
    echo 'Curl error: ' . curl_error($ch);
} else {
    echo $response;
}

curl_close($ch);

?>
