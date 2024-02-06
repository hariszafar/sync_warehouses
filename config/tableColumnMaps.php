<?php

/**
 * A multi-dimensional array mapping the column names in the source table (FileMaker)
 * to the column names in the destination table (Data Warehouse).
 * 
 * The first level array keys are the destination table names,
 * and the second level array keys are the source column names.
 */

return [
    'accesslog' => [
        "timestamp" => "date_created"
    ],
    'associate_documents' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "license_type" => "type",
        "license_number" => "documentnumber",
        "id_Entity" => "associateid",
        "entity_email" => "associateemail",
        "date_exp" => "date_expired"
    ],
    'documents' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "creationAccountName" => "created_by",
        "modificationAccountName" => "modified_by"
    ],
    'deposits' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'expenses' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'bills' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'billitems' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'contact_methods' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "Entities::type" => "contact_type",
        "isPrimary" => "is_primary"
    ],
    'valuelist' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'zipcodes' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created"
    ],
    'narrative_report' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "id_request" => "request_id"
    ],
    'therapist' =>  [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'users' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'reviewer' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'therapy_networks' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'payer' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'member' => [ // normalizing of column names for better readability
        "modificationHostTimestamp" => "date_modified",
        "nameFull" => "name"
    ],
    'request' => [
        "Entities Requests Reviewer Lookup::nameFull" => "reviewer",
        "Entities Requests Therapist::nameFull" => "therapist",
        "Entities Requests Therapist::primary_street_1" => "therapist_street",
        "Entities Requests Therapist::primary_state" => "therapist_state",
        "Entities Requests Therapist::primary_city" => "therapist_city",
        "Entities Requests Therapist::primary_zip" => "therapist_zip",
        "Entities Requests Nurse::nameFull" => "nurse",
        "payer_type" => "line_of_business",
        "Invoices::date_paid" => "date_paid",
        "payer_name" => "plan",
        "member_state" => "state",
        "creationHostTimestamp" => "date_created",
        "modificationHostTimestamp" => "date_modified",
        "creationAccountName" => "created_by",
        "long" => "lng",
        "amount_calc" => "amount"
    ],
    'request_subrequests' => [
        "modificationHostTimestamp" => "date_modified",
        "Requests::payer_name" => "payer_name",
        "Requests::assessment_id" => "assessment_id"
    ],
    'request_fees' => [
        "modificationHostTimestamp" => "date_modified",
        "creationHostTimestamp" => "date_created",
        "date" => "date_fee"
    ],
    'invoices' => [
        "balancePaymentsUnapplied" => "payments_unapplied",
        "Entities Invoices Payer::nameFull" => "plan",
        "Entities Invoices Payer::billing_payment_due" => "billing_payment_due",
        "modificationHostTimestamp" => "date_modified",
        "costSubtotal" => "cost_subtotal",
        "invoiceNumber" => "invoice_number",
        "billing_period_start" => "date_billing_start"
    ],
    'invoice_items' => [
        "modificationHostTimestamp" => "date_modified",
        "id_Request" => "id_request",
        "id_Invoice" => "id_invoice"
    ],
    'activity' => [
        "modificationHostTimestamp" => "date_modified"
    ],
    'locations' => [
        "modificationHostTimestamp" => "date_modified",
        "long" => "lng"
    ],
    'payments' => [
        "modificationHostTimestamp" => "date_modified",
        "modificationAccountName" => "modified_by"
    ],
    'invoice_activity' => [
        "modificationHostTimestamp" => "date_modified"
    ],
    'outcomes' => [
        "modificationHostTimestamp" => "date_modified"
    ],
    'managed_companies' => [
        "modificationHostTimestamp" => "date_modified"
    ],
    'lines_of_business' => [
        "modificationHostTimestamp" => "date_modified"
    ],

];