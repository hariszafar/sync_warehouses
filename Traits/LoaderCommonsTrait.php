<?php

trait LoaderCommonsTrait {
    
    public $rawData;
	public $table;
    public $primaryKeyColumn = null;
	public $masterTableAlias = 'm';
	public $temporaryTableAlias = 't';
	public $temporaryTable = '';
    public $lastProcessedTable = null;

    public $insertChunkSize = 50;
	

    /**
	 * Returns string with extra whitespaces removed.
	 * 
	 * @param string $string The string to be trimmed.
	 * @return string
	 */
	public function trimString(string $string): string
	{
		return trim(preg_replace('/[\r\n]+|[\s]{2,}/', ' ', $string));
	}

    /**
	 * Set a temporary table name based on the main table name and the current timestamp.
	 *
	 * @return void
	 */
	public function setTemporaryTableName(): void
	{
		$this->temporaryTable = $this->table . '_' . time();
	}

	/**
	 * Returns the name of the temporary table.
	 *
	 * @return string
	 */
	public function getTemporaryTableName(): string
	{
		if ($this->temporaryTable == '') {
			$this->setTemporaryTableName();
		}
		return $this->temporaryTable;
	}

	/**
	 * Resets the $temporaryTable property (used for storing the name of the temporary table).
	 * This should be triggered each time a new table is being updated.
	 * 
	 * @return void
	 */
	public function resetTemporaryTableName(): void
	{
		$this->temporaryTable = '';
	}

    /**
     * Returns the insertChunkSize parameter,
     * which is used to control how many records are attempted to be inserted together in a single chunk.
     *
     * @return integer
     */
    public function getInsertChunkSize(): int
    {
         return $this->insertChunkSize;
    }

    /**
     * Set the insertChunkSize parameter,
     * which is used to control how many records are attempted to be inserted together in a single chunk.
     *
     * @param integer $insertChunkSize
     * @return void
     */
    public function setInsertChunkSize(int $insertChunkSize): void
    {
        $this->insertChunkSize = $insertChunkSize;
    }

    /**
     * Adds the number of rows in the given chunk to the chunkRowCounts array.
     *
     * @param int $count The row count to be recorded for the current chunk.
     * @return void
     */
    public function addChunkRowCount(int $chunk): void
    {
        $this->chunkRowCounts[] = $chunk;
    }

	/**
	 * Clears the chunkRowCounts array.
	 * This should be triggered each time a new table is being updated, 
	 * so that the same chunk row counts aren't cached for the next table being updated.
	 * 
	 * @return void
	 */
	public function clearChunkRowCounts(): void
	{
		$this->chunkRowCounts = [];
	}

	/**
	 * Clears the sqlStatements array property.
	 *
	 * @return void
	 */
	public function clearSqlStatements(): void
	{
		$this->sqlStatements = [];
	}

	/**
	 * Returns the sqlStatements array property.
	 *
	 * @return array
	 */
	public function getSqlStatements(): array
	{
		return $this->sqlStatements;
	}
}