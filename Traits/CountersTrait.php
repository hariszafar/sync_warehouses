<?php

trait CountersTrait
{
    private $inputRows = 0;
    private $insertedRows = 0;
    private $updatedRows = 0;
    private $duplicateRows = 0;

    
    /**
     * Get the count of the input rows ($inputRows property)
     * 
     * @return int The count of the input rows ($inputRows property)
     */
    public function getInputRows(): int
    {
        return $this->inputRows;
    }

    /**
     * Get the count of the inserted rows ($insertedRows property)
     * 
     * @return int The count of the inserted rows ($insertedRows property)
     */
    public function getInsertedRows(): int
    {
        return $this->insertedRows;
    }

    /**
     * Get the count of the updated rows ($updatedRows property)
     * 
     * @return int The count of the updated rows ($updatedRows property)
     */
    public function getUpdatedRows(): int
    {
        return $this->updatedRows;
    }

    /**
     * Get the count of the duplicate rows ($duplicateRows property)
     *
     * @return int The count of the duplicate rows ($duplicateRows property)
     */
    public function getDuplicateRows(): int
    {
        return $this->duplicateRows;
    }

    /**
     * Get the total count of unchanged rows
     * 
     * @return int The total count of unchanged rows
     */
    public function getUnchangedRows(): int
    {
        return $this->getDuplicateRows() - $this->getUpdatedRows();
    }
    
    /**
     * Update the count of the input rows ($inputRows property)
     *
     * @param integer $count The count of the input rows
     * @return void
     */
    public function updateInputRows(int $count): void
    {
        $this->inputRows += $count;
    }

    /**
     * Update the count of the inserted rows ($insertedRows property)
     *
     * @param integer $count The count of the inserted rows
     * @return void
     */
    public function updateInsertedRows(int $count): void
    {
        $this->insertedRows += $count;
    }

    /**
     * Update the count of the updated rows ($updatedRows property)
     *
     * @param integer $count The count of the updated rows
     * @return void
     */
    public function updateUpdatedRows(int $count): void
    {
        $this->updatedRows += $count;
    }

    /**
     * Update the count of the duplicate rows ($duplicateRows property)
     *
     * @param integer $count The count of the duplicate rows
     * @return void
     */
    public function updateDuplicateRows(int $count): void
    {
        $this->duplicateRows += $count;
    }

    /**
     * Reset the counter for input rows ($inputRows property)
     *
     * @return void
     */
    private function resetInputRowsCounter(): void
    {
        $this->inputRows = 0;
    }

    /**
     * Reset the counter for inserted rows ($insertedRows property)
     *
     * @return void
     */
    private function resetInsertedRowsCounter(): void
    {
        $this->insertedRows = 0;
    }

    /**
     * Reset the counter for updated rows ($updatedRows property)
     *
     * @return void
     */
    private function resetUpdatedRowsCounter(): void
    {
        $this->updatedRows = 0;
    }

    /**
     * Reset the counter for duplicate rows ($duplicateRows property)
     *
     * @return void
     */
    private function resetDuplicateRowsCounter(): void
    {
        $this->duplicateRows = 0;
    }

    /**
     * Reset all the counters
     *
     * @return void
     */
    public function resetRecordCounters(): void
    {
        $this->resetInputRowsCounter();
        $this->resetInsertedRowsCounter();
        $this->resetUpdatedRowsCounter();
        $this->resetDuplicateRowsCounter();
    }

    /**
	 * Returns a non-associative array which contains the unchanged, inserted and changed rows counts respectively.
	 *
	 * @return array
	 */
	public function getAffectedRows(): array
	{
        return [
            $this->getUnchangedRows(),
            $this->getInsertedRows(),
            $this->getUpdatedRows()
        ];
    }

    /**
	 * Returns a log message summarizing the results of the updateTable operation.
	 *
	 * @return string
	 */
	public function getUpdateTableTaskSummaryLogMessage(): string
	{
		$logMessage = "Successfully updated RDS data." . PHP_EOL;
		$logMessage .= "Destination Table: " . $this->table . PHP_EOL;
		$logMessage .= "Input Rows: " . $this->getInputRows() . PHP_EOL;
		$logMessage .= "Unchanged Rows: " . $this->getUnchangedRows() . PHP_EOL;
		$logMessage .= "Inserted Rows: " . $this->getInsertedRows() . PHP_EOL;
		$logMessage .= "Updated Rows: " . $this->getUpdatedRows() . PHP_EOL;

		return $logMessage;
	}
}