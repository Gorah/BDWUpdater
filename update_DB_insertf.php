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
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
    $qry->bindParam(':name', $csv[2], PDO::PARAM_STR);
    $qry->bindParam(':surN', $csv[1], PDO::PARAM_STR);
    $qry->bindParam(':email', $csv[4], PDO::PARAM_STR);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertActions($arr, $actionT = 'New Hire'){
    
    $csv = $arr['data'];
    //    prepare insert into Actions table
    $SQL= "INSERT INTO tHR_Actions (EEID, ActionType, ReasonCode, StartDate, "
        ."EndDate, ModifiedDate, ModifiedBy, EmploymentStatus) VALUES (:eeID, "
        .":aT, :aR, '" .date('Y-m-d', strtotime($csv[12])) ."', "
        ."'9999-12-31', '" .date("Y-m-d H:i:s", time()) ."', 'Mass Upload', :eStat);";
    
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
    $qry->bindParam(':aT', $actionT, PDO::PARAM_STR);
    $qry->bindParam(':aR', $actionT, PDO::PARAM_STR);
    $qry->bindParam(':eStat', $csv[10], PDO::PARAM_STR);
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
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
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
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
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
     
//   !!!!!!!!!!  add job level to DB, gui and to this query.!!!!!!!!!
     
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
    $qry->bindParam(':eeID', $csv[31], PDO::PARAM_INT);
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

//***********************************************
//** End of Insert SQL functions for New Employee section
//***********************************************

?>
