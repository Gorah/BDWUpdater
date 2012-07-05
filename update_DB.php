<?php
include "../common/db_connection_PDO.php";

//function that dispaches task to correct action related functions and prepares 
//data for it to run. This is controller function for the whole update process
function run_update($inputData){
    switch($inputData['actionType']){
        case 'mig':
            $funct_name = 'migGetAction';
            break;
    }
    
    $data = array('path' => $inputData['filePath'],
                   'functName' => $funct_name,
                   'timeStamp' => $inputData['timeStamp']);
    getArray($data);
    
}


// data fetching from the csv file with data. With each row fetched function 
// fires off chain of update functions to apply data to the DB
function getArray($inputData){
//  file handle  pointing to uploaded file
    $fh = fopen($inputData['path'], "r");
    
    
//  parsing csv file - each row is imported as an array then sent to a function 
//  for further actions. Function name is passed in and chosen by the type of 
//  update run selected by the user.
    while (($data = fgetcsv($fh)) !== FALSE) {
        $fData = array('data' => $data,
                        'timeStamp' => $inputData['timeStamp']);
        call_user_func($inputData['functName'], $fData);
    }
    
}

//=============================================================================
// >>>MIG DATA UPLOAD PART
//=============================================================================


//    receives a row of data in form of an array and processes it against DB
function migGetAction($array){
//  test wheater employee is new hire or already exists in DB. Function will 
//  return TRUE if it's a new hire, FALSE if employee already exists under this ID
    $newEE = test_newHire($array['data'][3]);
    
    if ($newEE == TRUE){
        insert_NewEE($array);
    } else {
        update_EE($array);
    }
    
}

//**************************************
//** Add New Employee to DB Section
//**************************************

function test_newHire($eeID){
//    prepare SQL query to check if employee exist
    $SQL = "SELECT Count(ID) From tHR_Employee WHERE ID = " .$eeID;
    
//    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->execute();
    
//    get the result
    while($row = $qry->fetch(PDO::FETCH_NUM)){
        $hits = $row[0];
    }
    
//    determine boolean response: if query returned 0, it's new hire and value 
//    is TRUE, else it's an existing EE and value is FALSE
    if($hits != 0){
        $result = FALSE;
    } else {
        $result = TRUE;
    }
    
//    return the info to calling function
    return $result;
}

//wrapper function that fires off all the updates one after another ensuring 
//that all details are written to DB and are added in correct order
function insert_NewEE($csv){

    insertToEmployee($csv);
    insertActions($csv);
    insertActions($csv);
    insertOpsMru($csv);
    insertJobDetails($csv);
    
}

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

function insertActions($arr){
    
    $csv = $arr['data'];
    //    prepare insert into Actions table
    $SQL= "INSERT INTO tHR_Actions (EEID, ActionType, ReasonCode, StartDate, "
        ."EndDate, ModifiedDate, ModifiedBy, EmploymentStatus) VALUES (:eeID, "
        ."'New Hire', 'New Hire', '" .date('Y-m-d', strtotime($csv[12])) ."', "
        ."'9999-12-31', '" .date("Y-m-d H:i:s", time()) ."', 'Mass Upload', :eStat);";
    
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
    $qry->bindParam(':eStat', $csv[11], PDO::PARAM_STR);
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
    $qry->bindParam(':mru', $csv[8], PDO::PARAM_STR);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertJobDetails($arr){
     $csv = $arr['data'];
//    get info if employee is full time or part time
     $fPT = time_scale($csv[13]);
//    translate type of cotract from MIG ->BDW
     $wCtr = contract_Type($csv[0]);
     
//   !!!!!!!!!!  add job level to DB, gui and to this query.!!!!!!!!!
     
//     prepare insert query for Job Datails table. NOTE: cost center format of 
//     PLXXXXX... is used. To change it to CDXXXXX... format change $csv[7] to $csv[6]
    $SQL = "INSERT INTO tHR_JobDetails (EEID, Project, WorkContractType, CostCenter"
        .", JobCode, FTE, FullPartTime, ModifiedBy, StartDate, EndDate, ModifiedDate)"
        ."VALUES (:eeID, :mru, :workC, :cCentr, :job, :fte, :fPT, 'Mass Upload', '" 
        .date('Y-m-d', strtotime($csv[12])) ."', '9999-12-31', '" 
        .date("Y-m-d H:i:s", time()) ."');";
    
      //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
    $qry->bindParam(':mru', $csv[8], PDO::PARAM_STR);
    $qry->bindParam(':workC', $wCtr, PDO::PARAM_STR);
    $qry->bindParam(':cCentr', $csv[7], PDO::PARAM_STR);
    $qry->bindParam(':job', $csv[10], PDO::PARAM_STR);
    $qry->bindParam(':fte', $csv[13], PDO::PARAM_INT);
    $qry->bindParam(':fPT', $fPT, PDO::PARAM_STR);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

//***********************************************
//** End of Insert SQL functions for New Employee section
//***********************************************



//test if employee is Full Time or Part Time, basing on his FTE
function time_scale($FTE){
    
//    convert FTE from string to float to make check if FTE < 1, then it's part time
    $FTE = (float) $FTE;
    if ($FTE <1){
        return('Part Time');
    } else {
        return('Full Time');
    }
}

function contract_Type($val){
    //     translate value of work contract from MIG to BDW one
     if($val == 'Regular'){
         return("Permanent");
     } else {
         return $val;
     }
     
}

//**************************************
//** END OF: Add New Employee to DB Section
//**************************************

function update_EE($arr){
//    here add updating of the records for cost center, mru, job level/jobcode
    $mru = check_MRU($arr);
    if($mru == TRUE){
        update_MRU($arr);
    }
    
    
    $cCenter = check_costCenter($arr);
    
    if($cCenter == TRUE){
        update_CostCentr($arr);
    }
}


function update_CostCentr($arr){
//    function updating CC value for an employee
    $csv = $arr['data'];
    
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
    $SQL = "UPDATE tHR_JobDetails SET EndDate ='" .date("Y-m-d", time()) 
            ."' WHERE ID = :tID";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':tID', $targetID, PDO::PARAM_INT);
    $qry->execute();
    
//    inserting a new record with data from MIG. Note: this does mean that 
//    details like job title, EElevel, provider are going to be left blank. 
//    Inserting is done by re-using insert function
    insertJobDetails($csv);
    
}

function update_MRU($arr){
//    function updating MRU value for an employee
    $csv = $arr['data'];
    
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
    $SQL = "UPDATE tHR_JobDetails SET EndDate ='" .date("Y-m-d", time()) 
            ."' WHERE ID = :tID";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':tID', $targetID, PDO::PARAM_INT);
    $qry->execute();
    
//    inserting a new record with data from MIG. Note: this does mean that 
//    details like job title, EElevel, provider are going to be left blank. 
//    Inserting is done by re-using insert function
    insertJobDetails($csv);
    
}


//************************************
//supporting functions
//************************************


//function testing whether Cost Center for employee is up to date
function check_costCenter($arr){
    $csv = $arr['data'];
//    check if record newer than MIG file exists - if yes, return FALSE
     $SQL = "SELECT Count(EEID) FROM tHR_JobDetails WHERE EEID = :eeID"
            ." AND StartDate > '" .date("Y-m-d", $arr['timeStamp']) ."'";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
    $qry->execute();
    
     while($row = $qry->fetch(PDO::FETCH_NUM)){
        $hits = $row[0];
    }
    unset($qry);
    
    if(hits == 0){
        $SQL = "SELECT Count(EEID) FROM tHR_JobDetails WHERE EEID = :eeID AND"
            ." CostCenter = :cCentr AND EndDate > '" .date("Y-m-d", time()) ."'"
            ." AND StartDate < '" .date("Y-m-d", $arr['timeStamp']) ."'";
    
        $dbh = DB_con();
        $qry = $dbh->prepare($SQL);
        $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
        $qry->bindParam(':cCentr', $csv[7], PDO::PARAM_STR);
        $qry->execute();
    
        while($row = $qry->fetch(PDO::FETCH_NUM)){
            $hits = $row[0];
        }
        
         //    determine boolean response
        if($hits == 0){
            $result =TRUE;
        } else {
            $result = FALSE;
        }
        
    } else {
        $result = FALSE;
    }
   
    
//    return the info to calling function
    return $result;
}

//function testing whether MRU for employee is up to date
function check_MRU($arr){
   $csv = $arr['data'];
   //    check if record newer than MIG file exists - if yes, return FALSE
     $SQL = "SELECT Count(EEID) FROM tHR_JobDetails WHERE EEID = :eeID"
            ." AND StartDate > '" .date("Y-m-d", $arr['timeStamp']) ."'";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
    $qry->execute();
    
     while($row = $qry->fetch(PDO::FETCH_NUM)){
        $hits = $row[0];
    }
    unset($qry);
    
    if(hits == 0){
        $SQL = "SELECT Count(EEID) FROM tHR_JobDetails WHERE EEID = :eeID AND"
            ." Project = :MRU AND EndDate > '" .date("Y-m-d", time()) ."'"
            ." AND StartDate < '" .date("Y-m-d", $arr['timeStamp']) ."'";
    
        $dbh = DB_con();
        $qry = $dbh->prepare($SQL);
        $qry->bindParam(':eeID', $csv[3], PDO::PARAM_INT);
        $qry->bindParam(':MRU', $csv[8], PDO::PARAM_STR);
        $qry->execute();
    
        while($row = $qry->fetch(PDO::FETCH_NUM)){
            $hits = $row[0];
        }
        
         //    determine boolean response
        if($hits == 0){
            $result =TRUE;
        } else {
            $result = FALSE;
        }
        
    } else {
        $result = FALSE;
    }
   
    
//    return the info to calling function
    return $result;
}

//=============================================================================
// >>>GTT UPLOAD PART
//=============================================================================


//stuff yet to come
?>
