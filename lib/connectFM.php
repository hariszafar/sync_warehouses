<?php
class connectFM
{
    use FmdataTrait;

    protected $layout = '@ Requests_KPI';

	public function __construct($config)
	{
        FmdataManager::setConfig($config);
	}

    public function setLayout($layout)
    {
        $this->layout = $layout;
        return $this;
    }
}
