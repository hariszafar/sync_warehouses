<?php

interface Loader
{
    public function getLastSyncedTimestamp($table, $subtractDay = true);
    public function updateLastSyncedTimestamp($table, $timestamp);
    // public function updateTable();
    public function setOffsetDatetime($offsetDatetime);
}