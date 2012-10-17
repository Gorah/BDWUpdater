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
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
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
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
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
    $qry->bindParam(':mail', $csv[4], PDO::PARAM_STR);
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


function terminateEE($arr){
    $csv = $arr['data'];
    $SQL = "SELECT TOP(1) ID FROM tHR_Actions WHERE EEID=:id ORDER BY EndDate DESC";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindPAram(':id', $csv[1], PDO::PARAM_STR);
    $qry->execute();
    
    while ($row = $qry->fetch(PDO::FETCH_NUM)) {
        $id=$row[0];
    }
    unset($qry);

    //calculating end date for delimited records and start date of termination
    $startOfTermination = substr($arr['timeStamp'], 0,-2) . '01';
    $endOfEmployment = strtotime('-1 day', strtotime( $startOfTermination));
    $startOfTermination = date('Y-m-d', strtotime($startOfTermination));
    $endOfEmployment = date('Y-m-d', $endOfEmployment);

        
     //delimiting current record in tHR_Actions
     $qry=$dbh->prepare("UPDATE tHR_Actions SET EndDate=:eDate, EmploymentStatus ="
             . "'Terminated' WHERE ID=:eeID");
     $qry->bindParam(':eDate', $endOfEmployment, PDO::PARAM_STR);
     $qry->bindParam(':eeID', $id, PDO::PARAM_INT);
     
     try{
        $qry->execute();
     } catch (PDOException $err) {
        $fh = fopen('/logs/error.log', 'a');
        $msg = date("d-m-Y H:i:s ", time()) ." Error: UPDATE failed. tHR_Employee. "
                ."EEID - " .$csv[1] ."; " .$err;
        fwrite($fh, $msg);
        fclose($fh);
     }
     
     unset($qry);
        
     //adding new record into tHR_Actions holding termination details
     $SQL = "INSERT INTO tHR_Actions (EEID, ActionType, ReasonCode, "
          . "StarDate, EndDate, ModifiedDate, ModifiedBy, EmploymentStatus) "
          . "VALUES (:eeID, 'Termination', 'Left Company', :sDate, '9999-12-31', '"
          . date('Y-m-d'). "', 'Mass Upload', 'Terminated');";

        $qry=$dbh->prepare($SQL);
        $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
        $qry->bindParam(':sDate', $startOfTermination, PDO::PARAM_STR);
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
     
//      finding record to delimit in team assigment table (tHR_TeamMembers)  
        $SQLstr = "SELECT TOP(1) ID FROM tHR_TeamMembers WHERE EEID=:eeID ORDER BY EndDate DESC";
        $qry = $dbh->prepare($SQLstr);
        $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
        $qry->execute();
        
        $id = 0;
        while ($row = $qry->fetch(PDO::FETCH_NUM)) {
            $id=$row[0];
        }
        unset($qry);
        
        //delimiting current record in tHR_TeamMembers
        $qry=$dbh->prepare("UPDATE tHR_TeamMembers SET EndDate=:eDate WHERE ID=:ID");
        $qry->bindParam(':eDate', $endOfEmployment, PDO::PARAM_STR);
        $qry->bindParam(':ID', $id, PDO::PARAM_INT);
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
}

function updateJobDetails($csv, $id, $end, $start){
    
    //delimiting old record
    $SQL = "UPDATE tHR_JobDetails SET EndDate = :eDate WHERE ID = :id";
    $dbh = DB_Con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eDate', $end, PDO::PARAM_STR);
    $qry->bindParam(':id', $id, PDO::PARAM_STR);
    $qry->execute();
    
    //inserting new record to tHR_JobDetails
    insertNewJob($csv, $start);
    //delimiting tHR_Actions
    updateAction($csv[1], $end);
    //inserting new action tHR_Actions
    insertNewAction($csv, $start);
}

function insertNewJob($csv, $start){
    //    get info if employee is full time or part time
     $fPT = time_scale($csv[11]);
//    translate type of cotract from MIG ->BDW
     $wCtr = contract_Type($csv[0]);
     
//   !!!!!!!!!!  add job level to DB, gui and to this query.!!!!!!!!!
     
//     prepare insert query for Job Datails table. NOTE: cost center format of 
//     PLXXXXX... is used. To change it to CDXXXXX... format change $csv[8] to $csv[6]
    $SQL = "INSERT INTO tHR_JobDetails (EEID, Project, WorkContractType, CostCenter"
        .", JobCode, FTE, FullPartTime, ModifiedBy, StartDate, EndDate, ModifiedDate)"
        ."VALUES (:eeID, :mru, :workC, :cCentr, :job, :fte, :fPT, 'Mass Upload', '" 
        .date('Y-m-d', strtotime($start)) ."', '9999-12-31', '" 
        .date("Y-m-d H:i:s", time()) ."');";
    
      //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
    $qry->bindParam(':mru', $csv[7], PDO::PARAM_STR);
    $qry->bindParam(':workC', $wCtr, PDO::PARAM_STR);
    $qry->bindParam(':cCentr', $csv[5], PDO::PARAM_STR);
    $qry->bindParam(':job', $csv[9], PDO::PARAM_STR);
    $qry->bindParam(':fte', $csv[11], PDO::PARAM_INT);
    $qry->bindParam(':fPT', $fPT, PDO::PARAM_STR);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function updateAction($eeid, $end){
     //finding record ID to delimit 
    $SQL = "SELECT TOP 1 ID FROM tHR_Actions WHERE EEID = :id "
        . "ORDER BY StartDate Desc"; 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':id', $eeid, PDO::PARAM_INT);
    $qry->execute();
        
    while($row = $qry->fetch(PDO::FETCH_NUM)){
        $id = $row[0]; 
    }
    //delimiting old record
    $SQL = "UPDATE tHR_JobDetails SET EndDate = :eDate WHERE ID = :id";
    $dbh = DB_Con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eDate', $end, PDO::PARAM_STR);
    $qry->bindParam(':id', $id, PDO::PARAM_STR);
    $qry->execute();
}

function insertNewAction($csv, $start){
     $SQL= "INSERT INTO tHR_Actions (EEID, ActionType, ReasonCode, StartDate, "
        ."EndDate, ModifiedDate, ModifiedBy, EmploymentStatus) VALUES (:eeID, "
        ."'Job Change', 'Job Change', '" .date('Y-m-d', strtotime($start)) ."', "
        ."'9999-12-31', '" .date("Y-m-d H:i:s", time()) ."', 'Mass Upload', :eStat);";
    
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
    $qry->bindParam(':eStat', $csv[10], PDO::PARAM_STR);
    $qry->execute();
}

function removeFromTeam($eeid, $end){
    //  finding record to delimit in team assigment table (tHR_TeamMembers)  
        $SQLstr = "SELECT TOP(1) ID FROM tHR_TeamMembers WHERE EEID=:eeID ORDER BY EndDate DESC";
        $qry = $dbh->prepare($SQLstr);
        $qry->bindParam(':eeID', $eeid, PDO::PARAM_INT);
        $qry->execute();
        
        while ($row = $qry->fetch(PDO::FETCH_NUM)) {
            $id=$row[0];
        }
        unset($qry);
        
        //delimiting current record in tHR_TeamMembers
        $qry=$dbh->prepare("UPDATE tHR_TeamMembers SET EndDate=:eDate WHERE ID=:ID");
        $qry->bindParam(':eDate', $end, PDO::PARAM_STR);
        $qry->bindParam(':ID', $id, PDO::PARAM_INT);
}

function addToNewMRU($eeID, $MRU){
      //   insert EE ID into OpsMRU table to identify which MRU employee belongs to 
//   before he's added to his work group
    $SQL = "INSERT INTO tHR_OpsMRU (ID, MRU) VALUES (:eeID, :mru);";
    
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $eeID, PDO::PARAM_INT);
    $qry->bindParam(':mru', $MRU, PDO::PARAM_STR);
    $qry->execute();
}
?>
