<?php
include "../common/db_connection_PDO.php";

function run_update($inputData){
    switch($inputData['actionType']){
        case 'mig':
            $funct_name = 'migGetAction';
            break;
    }
    
    $data = array('path' => $inputData['filePath'],
                   'functName' => $funct_name);
    getArray($data);
    
}


function getArray($inputData){
//  file handle  pointing to uploaded file
    $fh = fopen($inputData['path'], "r");
    
    
//  parsing csv file - each row is imported as an array  
    while (($data = fgetcsv($fh)) !== FALSE) {
        call_user_func($inputData['functName'], $data);
    }
    
}

//    receives a row of data in form of an array and processes it against DB
function migGetAction($array){
//  test wheater employee is new hire or already exists in DB. Function will 
//  return TRUE if it's a new hire, FALSE if employee already exists under this ID
    $newEE = test_newHire($array[3]);
    
    if ($newEE == TRUE){
        insert_NewEE($array);
    } else {
        update_EE($array);
    }
    
}


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


function insert_NewEE($csv){

    insertToEmployee($csv);
    insertActions($csv);
    insertActions($csv);
    insertOpsMru($csv);
    insertJobDetails($csv);
    
}

function insertToEmployee($csv){
    //    prepare insert for main EE table
    $SQL = "INSERT INTO tHR_Employee (ID, FirstName, LastName, Email) VALUES ("
        . $csv[3] .", '" .$csv[2] ."', '" .$csv[1] ."', '" .$csv[4] ."');";
    
    //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertActions($csv){
    //    prepare insert into Actions table
    $SQL= "INSERT INTO tHR_Actions (EEID, ActionType, ReasonCode, StartDate, "
        ."EndDate, ModifiedDate, ModifiedBy, EmploymentStatus) VALUES (" .$csv[3]
        .", 'New Hire', 'New Hire', '" .date('Y-m-d', strtotime($csv[12])) ."', "
        ."'9999-12-31', '" .date("Y-m-d H:i:s", time()) ."', 'Mass Upload', '"
        .$csv[11] ."');";
    
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertDate($csv){
    //    insert original hire date into Date Specs table
    $SQL = "INSERT INTO tHR_DateSpecs (EmpID, DateType, DateValue) VALUES (" 
        .$csdv[3] .", 'Original Hire Date', '" .date('Y-m-d', strtotime($csv[12]))
        . "');";
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertOpsMru($csv){
    //   insert EE ID into OpsMRU table to identify which MRU employee belongs to 
//   before he's added to his work group
    $SQL = "INSERT INTO tHR_OpsMRU (ID, MRU) VALUES (" .$csv[3] .", '" .$csv[8]
        ."');";
    
     //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

function insertJobDetails($csv){
//    get info if employee is full time or part time
     $fPT = time_scale($csv[13]);
//    translate type of cotract from MIG ->BDW
     $wCtr = contract_Type($csv[0]);
     
     
//     prepare insert query for Job Datails table. NOTE: cost center format of 
//     PLXXXXX... is used. To change it to CDXXXXX... format change $csv[7] to $csv[6]
    $SQL = "INSERT INTO tHR_JobDetails (EEID, Project, WorkContractType, CostCenter"
        .", JobCode, FTE, FullPartTime, ModifiedBy, StartDate, EndDate, ModifiedDate)"
        ."VALUES (" .$csv[3] .", '" .$csv[8] ."', '" .$wCtr ."', '" .$csv[7] ."', '"
        .$csv[10] ."', " .$csv[13] .", '" .$fPT ."', 'Mass Upload', '" .date('Y-m-d', strtotime($csv[12]))
        ."', '9999-12-31', '" .date("Y-m-d H:i:s", time()) ."');";
    
      //    run the query 
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->execute();
    
    unset($qry);
    unset($dbh);
}

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


function update_EE($csv){
    
}
?>