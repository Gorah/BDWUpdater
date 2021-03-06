<?php

//***********************************************
//** Insert SQL functions for New Employee section
//***********************************************

function insertToEmployee($arr){
    
    $csv = $arr['data'];
    //    prepare insert for main EE table
    $SQL = "INSERT INTO tHR_Employee (ID, FirstName, LastName, Email) VALUES ("
        . ":eeID, :name, :surN, :email);";
    
    //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
    $qry->bindParam(':name', $csv[3], PDO::PARAM_STR);
    $qry->bindParam(':surN', $csv[2], PDO::PARAM_STR);
    $qry->bindParam(':email', $csv[4], PDO::PARAM_STR);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertActions($arr, $actionT = 'New Hire'){
    
    $csv = $arr['data'];
    
    if ($actionT == 'Rehire' || $actionT == 'Leave of Absence' || $actionT = 'Leave With Pay' || $actionT = 'Return From LOA'){
        closeActionRecord($csv);
    }
    
    if ($csv[10] == 'Leave of Absence'){
        $eStat = 'LOA';
    } elseif ($csv[10] == 'Leave With Pay'){
        $eStat = 'LwP';
    } else {
        $eStat = 'Active';
    }
    
    //    prepare insert into Actions table
    $SQL= "INSERT INTO tHR_Actions (EEID, ActionType, ReasonCode, StarDate, "
        ."EndDate, ModifiedDate, ModifiedBy, EmploymentStatus) VALUES (:eeID, "
        .":aT, :aR, '" .date('Y-m-d', strtotime($csv[12])) ."', "
        ."'9999-12-31', '" .date("Y-m-d H:i:s", time()) ."', 'Mass Upload', :eStat);";
    
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
    $qry->bindParam(':aT', $actionT, PDO::PARAM_STR);
    $qry->bindParam(':aR', $actionT, PDO::PARAM_STR);
    $qry->bindParam(':eStat', $eStat, PDO::PARAM_STR);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertDate($arr){
     $csv = $arr['data'];
    //    insert original hire date into Date Specs table
    $SQL = "INSERT INTO tHR_DateSpecs (EmpID, DateType, DateValue) VALUES (:eeID,"
        ."'Original Hire Date', '" .date('Y-m-d', strtotime($csv[12])) . "');";
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertOpsMru($arr){
     $csv = $arr['data'];
    //   insert EE ID into OpsMRU table to identify which MRU employee belongs to 
//   before he's added to his work group
    $SQL = "INSERT INTO tHR_OpsMRU (ID, MRU) VALUES (:eeID, :mru);";
    
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
    $qry->bindParam(':mru', $csv[7], PDO::PARAM_STR);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertJobDetails($arr){
     $csv = $arr['data'];
//    get info if employee is full time or part time
     $fPT = time_scale($csv[11]);
//    translate type of cotract from MIG ->BDW
     $wCtr = contract_Type($csv[0]);
     
     
//     prepare insert query for Job Datails table. NOTE: cost center format of 
//     PLXXXXX... is used. To change it to CDXXXXX... format change $csv[6] to $csv[6]
    $SQL = "INSERT INTO tHR_JobDetails (EEID, Project, WorkContractType, CostCenter"
        .", JobCode, FTE, FullPartTime, ModifiedBy, StartDate, EndDate, ModifiedDate)"
        ."VALUES (:eeID, :mru, :workC, :cCentr, :job, :fte, :fPT, 'Mass Upload', '" 
        .date('Y-m-d', strtotime($csv[12])) ."', '9999-12-31', '" 
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

//delimits record in tHR_Actions
function closeActionRecord($csv){
    
    $SQL = "SELECT TOP 1 ID FROM tHR_Actions WHERE EEID =:id ORDER BY EndDate DESC";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':id', $csv[1], PDO::PARAM_INT);
    $qry->execute();
    
    while($row = $qry->fetch(PDO::FETCH_NUM)){
        $targetID = $row[0];
    }
    
    unset($qry);
    unset($dbh);
    
    $endDate = strtotime('-1 day', strtotime($csv[12]));
    $endDate = date('Y-m-d', $endDate);
//    delimiting newest record for the employee
    $SQL = "UPDATE tHR_Actions SET EndDate ='" .$endDate 
            ."' WHERE ID = :tID";
    $dbh = DB_Con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':tID', $targetID, PDO::PARAM_INT);
    $qry->execute();
}
//***********************************************
//** End of Insert SQL functions for New Employee section
//***********************************************

?>
