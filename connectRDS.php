<?php
class createCon
{
    protected $host;
    protected $user;
    protected $pass;
    protected $db;
    protected $myconn;

    public function __construct($config)
    {
        $this->host = $config['host'];
        $this->user = $config['user'];
        $this->pass = $config['pass'];
        $this->db = $config['db'];
    }

    public function connect() {
        $con = mysqli_connect($this->host, $this->user, $this->pass, $this->db);
        if (!$con) {
            die('Could not connect to database!');
        } else {
            $this->myconn = $con;
            //echo 'Connection established!';}
            return $this->myconn;
        }
    }

    public function close() {
        mysqli_close($this->myconn);
        echo 'Connection closed!';
    }
}
