<?php

/**
 * This file contains the list of tables that should not have the day subtraction logic applied to them
 * when fetching the last synced timestamp from the database (for the next sync).
 */

return [
    "zipcodes",
];