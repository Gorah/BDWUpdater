<?php

function update_CostCentr($arr){
//    function updating CC value for an employee
    $csv = $arr['data'];
//  get end date by taking report time stamp and substracting one day from it  
    $endDate = date("Y-m-d", strtotime('-1 day', strtotime($arr[timeStamp])));
//    finding ID of the row to delimit
    $SQL = "SELECT TOP 1 ID FROM tHR_JobDetails WHERE EEID = :eeid ORDER BY EndDate DESC";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
    $qry->execute();
    
    while($row = $qry->fetch(PDO::FETCH_NUM)){
        $targetID = $row[0];
    }
    
    unset($qry);
    
//    delimiting newest record for the employee
    $SQL = "UPDATE tHR_JobDetails SET EndDate ='" .$endDate 
            ."' WHERE ID = :tID";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':tID', $targetID, PDO::PARAM_INT);
    $qry->execute();
    
//    inserting a new record with data from MIG. Note: this does mean that 
//    details like job title, EElevel, provider are going to be left blank. 
//    Inserting is done by re-using insert function
    insertJobDetails($csv);
    
    unset($qry);
    unset($dbh);
    
}

function update_MRU($arr){
//    function updating MRU value for an employee
    $csv = $arr['data'];
    $endDate = date("Y-m-d", strtotime('-1 day', strtotime($arr[timeStamp])));
    
//    finding ID of the row to delimit
    $SQL = "SELECT TOP 1 ID FROM tHR_JobDetails WHERE EEID = :eeid ORDER BY EndDate DESC";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
    $qry->execute();
    
    while($row = $qry->fetch(PDO::FETCH_NUM)){
        $targetID = $row[0];
    }
    
    unset($qry);
    
//    delimiting newest record for the employee
    $SQL = "UPDATE tHR_JobDetails SET EndDate ='" .$endDate 
            ."' WHERE ID = :tID";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':tID', $targetID, PDO::PARAM_INT);
    $qry->execute();
    
//    inserting a new record with data from MIG. Note: this does mean that 
//    details like job title, EElevel, provider are going to be left blank. 
//    Inserting is done by re-using insert function
    insertJobDetails($csv);
    
    unset($qry);
    unset($dbh);
}

function updateEEDetails($csv){
//    function to update user details
    
    $SQL = "UPDATE tHR_Employee SET LastName = :lName, Email = :mail WHERE ID = :eeID";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
    $qry->bindParam(':lName', $csv[2], PDO::PARAM_STR);
    $qry->bindParam(':mail', $csv[3], PDO::PARAM_STR);
//    error catching and writing in the error log
    try{
        $qry->execute();
    }
    catch (PDOException $err) {
        $fh = fopen('/logs/error.log', 'a');
        $msg = date("d-m-Y H:i:s ", time()) ." Error: UPDATE failed. tHR_Employee. "
                ."EEID - " .$csv[1] ."; " .$err;
        fwrite($fh, $msg);
        fclose($fh);
    }
    
    unset($qry);
    unset($dbh);
}

?>
