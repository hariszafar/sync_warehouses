<?php
class FmdataManager
{

    protected static $fm;
    protected static $config = [];

    public static function get($reuse = true)
	{
        if (!isset(self::$fm) || false === $reuse) {
            require_once(__DIR__ . '/FmRest.php');
            self::$fm = new FmRest();
            self::$fm->setHost(self::$config['fmdb_host'])
            ->setDb(self::$config['fmdb_database']);
        }
        return self::$fm;
    }

    public static function setConfig(array $config)
    {
        self::$config = $config;
    }
}
