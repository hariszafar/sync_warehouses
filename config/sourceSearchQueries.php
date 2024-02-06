<?php

return [
    'accesslog' => [
        ['timestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'associate_documents' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'documents' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER],
        ['filename' => "=", "omit" => 'true']
       ],
    'deposits' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'expenses' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'bills' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'billitems' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'contact_methods' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'valuelist' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'zipcodes' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'narrative_report' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'therapist' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER, 'type' => '=therapist'],
        ['nameFull' => "=", 'omit' => 'true']
    ],
    'users' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER, 'type' => '=user'],
        ['nameFull' => "=", 'omit' => 'true']
    ],
    'reviewer' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER, 'type' => '=reviewer'],
        ['nameFull' => "=", 'omit' => 'true']
    ],
    'therapy_networks' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
       ],
    'payer' => [
        ['category' => '=Payer']
    ],
    'member' => [
        ['type' => '=Patient']
    ],
    'request' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER],
        //['id' => '> 0'],
        ['payer_name' => 'Test', 'omit' => 'true']
    ],
    'request_subrequests' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER],
        ['Requests::payer_name' => 'Test', 'omit' => 'true'],
        ['additional_consideration' => "1", 'omit' => 'true']
    ],
    'request_fees' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'invoices' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'invoice_items' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'activity' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'locations' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'payments' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'invoice_activity' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
    'outcomes' => [
        ['id' => "> 0"]
    ],
    'managed_companies' => [
        ['id' => "> 0"]
    ],
    'lines_of_business' => [
        ['modificationHostTimestamp' => DATETIME_SEARCH_PLACEHOLDER]
    ],
];