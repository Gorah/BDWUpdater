<?php
include "../common/db_connection_PDO.php";
//include insert/update functions used by this tool to operate on DB
include 'update_DB_insertf.php';
include 'update_DB_udatef.php';

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

//wrapper function that fires off all the updates one after another ensuring 
//that all details are written to DB and are added in correct order
function insert_NewEE($csv){

    insertToEmployee($csv);
    insertActions($csv);
    insertActions($csv);
    insertOpsMru($csv);
    insertJobDetails($csv);
    
}

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


//************************************
//supporting functions - MIG
//************************************

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
            ." CostCenter = :cCentr AND EndDate > '" .date("Y-m-d", $arr['timeStamp']) ."'"
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
            ." Project = :MRU AND EndDate > '" .date("Y-m-d", $arr['timeStamp']) ."'"
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
