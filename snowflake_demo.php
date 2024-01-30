<?php

$startTime = time();
ini_set('display_errors', 1);
error_reporting(E_ALL);


include "snowflakeLoader.php";

echo "Snowflake script called\n";
/* try {
    // $dbh = new PDO("snowflake:account=$account", $user, $password);

    // $dbh = new PDO("odbc:".SNOWFLAKE_ODBC_DSN, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD);
    $dbh = new PDO("snowflake:account=".SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD);
    $dbh->setAttribute( PDO::ATTR_CURSOR, PDO::CURSOR_SCROLL );
    $dbh->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION );
    echo "Connected<br />";

    //Connect to Warehouse
    echo $query = "USE WAREHOUSE " . SNOWFLAKE_WAREHOUSE ;
    $stmt = $dbh->prepare($query);
    $stmt->execute();
    //Connect to Database
    $query = "USE DATABASE " . SNOWFLAKE_DATABASE ;
    $stmt = $dbh->prepare($query);
    $stmt->execute();
    //Connect to Schema
    $query = "USE SCHEMA " . SNOWFLAKE_SCHEMA;
    $stmt = $dbh->prepare($query);
    $stmt->execute();

    echo "<br /><br />=======================";
    echo "<br />BEFORE CREATING TABLE";
    echo "<br />=======================<br /><br />";
    $sth = $dbh->query("SHOW tables");
    echo "<pre>";
    while ($row = $sth->fetchAll(PDO::FETCH_ASSOC)) {
        
        echo "RESULT: " . var_dump($row) . "<br />";
    }
    echo "</pre>";

    // Create a table
    $createTableQuery = "CREATE TABLE IF NOT EXISTS TEST_STUDENT_UPLOAD_TABLE (
        ID STRING CONSTRAINT id_primary_key PRIMARY KEY,
        Name STRING,
        Discipline STRING,
        Active BOOLEAN
    );";
    $stmt = $dbh->prepare($createTableQuery);
    echo "Table creation query: {$createTableQuery} <br />";
    $stmt->execute();
    echo "Table creation query executed.<br />";
    echo "<br/><br/>=======================";
    echo "<br />AFTER CREATE TABLE QUERY ATTEMPT";
    echo "<br />=======================<br /><br />";
    $sth = $dbh->query("show tables;");
    echo "<pre>";
    while ($row=$sth->fetch(PDO::FETCH_ASSOC)) {
        
        echo "RESULT: " . print_r($row) . "<br />";
    }
    echo "</pre>";


    echo "<br /><br />=======================";
    echo "<br />INSERT STUDENT RECORDS";
    echo "<br />=======================<br /><br />";

    // Sample students data
    $studentsData = [
        ['S001', 'John Doe', 'Computer Science', true],
        ['S002', 'Jane Smith', 'Mathematics', true],
        ['S003', 'Bob Johnson', 'Physics', false],
        ['S004', 'Alice Williams', 'Chemistry', true],
        ['S005', 'David Brown', 'Biology', false],
    ];

    // Prepare the SQL statement with the ON DUPLICATE KEY UPDATE directive
    $sql = "INSERT INTO TEST_STUDENT_UPLOAD_TABLE (ID, Name, Discipline, Active)
    VALUES (:id, :name, :discipline, :active);";


    // Insert each record using the prepared statement
    foreach ($studentsData as $student) {
        $stmt = $dbh->prepare($sql);
        $stmt->bindParam(':id', $student[0]);
        $stmt->bindParam(':name', $student[1]);
        $stmt->bindParam(':discipline', $student[2]);
        $stmt->bindParam(':active', $student[3], PDO::PARAM_BOOL);
        
        $stmt->execute();
    }

    echo "Records inserted successfully.";

    $dbh = null;
    echo "<br />***Script Execution Completed in: <strong> " . (time() - $startTime) ."seconds <strong>";
} catch (PDOException $e) {
    echo "Error: ". $e->getMessage() ."";
} */

try {
    $snow = new SnowflakeLoader();
    $studentsData = [
        ['ID' => 'S001', 'Name' => 'John Doe', 'Discipline' => 'Computer Science', 'Active' => 1],
        ['ID' => 'S002', 'Name' => 'Jane Smith', 'Discipline' => 'Mathematics', 'Active' => 1],
        ['ID' => 'S003', 'Name' => 'Bob Johnson', 'Discipline' => 'Physics', 'Active' => 0],
        ['ID' => 'S004', 'Name' => 'Charlie Brown', 'Discipline' => 'Chemistry', 'Active' => 1],
        ['ID' => 'S005', 'Name' => 'David Brown', 'Discipline' => 'Biology', 'Active' => 0],
        ['ID' => 'S006', 'Name' => 'Haris Zafar', 'Discipline' => 'Computer Science', 'Active' => 1],
        ['ID' => 'S007', 'Name' => 'Haris Zafar 2', 'Discipline' => 'Computer Science', 'Active' => 1],
        ['ID' => 'S008', 'Name' => 'Mr. Cheese Burger', 'Discipline' => 'Food Sciences', 'Active' => 1],
    ];
    $rawData = json_encode($studentsData);
    $table = 'TEST_2_STUDENT_UPLOAD_TABLE';
    $primaryKeyColumn = 'ID';
    $snow->updateTable($rawData, $table, $primaryKeyColumn);
    echo "Script execution complete. Verify data on Snowflake.\n";
} catch (\Throwable $th) {
    echo $th->getMessage();
}
?>