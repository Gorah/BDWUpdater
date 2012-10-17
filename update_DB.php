<?php
include '../common/connect_for_uploader.php';
//include insert/update functions used by this tool to operate on DB
include './update_DB_insertf.php';
include './update_DB_updatef.php';

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

     $fh = fopen('./logs/update.log', 'a');
     $msg = date("d-m-Y H:i:s ", time()) ." Update Complete. "
           ."Update type performed: " .$inputData['actionType'] . PHP_EOL;
     fwrite($fh, $msg);
     fclose($fh);

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

    $newEE = test_newHire($array['data'][1]);

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
    insertDate($csv);
    insertOpsMru($csv);
    insertJobDetails($csv);

}

function update_EE($arr){
    
//  $action variable holds info which action to perfom during update
    $action = checkIfTerminated($arr['data'][1], $arr['timeStamp'], $arr['data'][10]);
    
    switch($action){
        case 'update':
            //update action in tHR_Employee
            checkEEdetails($arr['data']);
            //update action in tHR_JobDetails
            checkJobChange($arr);
            break;
        case 'terminate':
            //terminating employee
            terminateEE($arr);
            break;
        case 'rehire':
            //rehire action
            insertActions($arr, 'Rehire');
            //inserting employee to tHR_OpsMRU so he appears in his MRU basket
            insertOpsMru($arr);
            break;
        case 'end':
            break;
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

    $hits = 0;

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

//a function to test what sort of update function to commit on the employee
//it's comparing the state of EE in BDW and MIG. 
//If Employee is:
//  * Active in both -> regular update function chosen
//  * Active in BDW, not in MIG -> termination action performed
//  * Terminated in BDW, not in MIG -> re-hire action performed
//  * Terminated in both -> no action taken
//Only the newest record from BDW is taken into consideration while comparing
//employment statuses between DB's.
//Function returns string with action type to take.
function checkIfTerminated($id, $stampDate, $eeStat){
    
    //fetching newest employment status (the one with StartDate same or newer than
    //upload file's stamp date
    $SQL = "SELECT TOP 1 EmploymentStatus FROM tHR_Actions WHERE EEID = :id AND "
        . "StarDate >= :stamp ORDER BY StarDate DESC";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':id', $id, PDO::PARAM_INT);
    $qry->bindParam(':stamp', $stampDate, PDO::PARAM_STR);
    $qry->execute();
    
    $status = '';
    while($row=$qry->fetch(PDO::FETCH_NUM)){
        $status = $row[0];
    }
    
    unset($qry);
    unset($dbh);
    
    //if there were no results (ie: latest record is older than stamp date of the
    //upload file) new fetch is prepared to get nmost up-to-date record
    if ($status =='' || $status == NULL){
         $SQL = "SELECT TOP 1 EmploymentStatus FROM tHR_Actions WHERE EEID = :id "
        . "ORDER BY StarDate DESC";
        $dbh = DB_con();
        $qry = $dbh->prepare($SQL);
        $qry->bindParam(':id', $id, PDO::PARAM_INT);
        $qry->execute();

        $status = '';
        while($row=$qry->fetch(PDO::FETCH_NUM)){
            $status = $row[0];
        }
    }
    
//  basing on the employee status from BDW we're making compare with MIG status
//  and using mentioned above criteria we're choosing action to perform  
    switch($status){
        case 'Active':
            if  ($eeStat == 'Active'){
                return 'update';
            } else {
                return 'terminate';
            }
            break;
        case 'Terminated':
            if  ($eeStat == 'Terminated'){
                return 'end';
            } else {
                return 'rehire';
            }
            break;
    }
    
}


function checkJobChange($arr){
     $csv = $arr['data'];
     
     
     //find if record with same data already exists in DB
     $SQL = "SELECTCount(ID) FROM tHR_JobDetails WHERE EEID = :id AND EndDate >= :date "
        . "AND Project = :proj AND CostCenter = :cc AND FTE = :fte AND "
        . "JobCode = :jCode";
     
     $dbh = DB_con();
     $qry = $dbh->prepare($SQL);
     $qry->bindParam(':id', $csv[1], PDO::PARAM_INT);
     $qry->bindParam(':date', $arr['timeStamp'], PDO::PARAM_STR);
     $qry->bindParam(':proj', $csv[7], PDO::PARAM_STR);
     $qry->bindParam(':cc', $csv[5], PDO::PARAM_STR);
     $qry->bindParam(':fte', $csv[11], PDO::PARAM_INT);
     $qry->bindParam(':jCode', $csv[9], PDO::PARAM_STR);
     
     $qry->execute();
     
     $hits = 0;
     while($row = $qry->fetch(PDO::FETCH_NUM)){
         $hits = $row[0]; 
     }
     unset($qry);
     unset($dbh);
     
     //if $hits = 0, it means that there's no such record in DB and update action
     //is necessary
     if (0 == $hits) {
        
        //finding record ID to delimit 
        $SQL = "SELECT TOP 1 ID FROM tHR_JobDetails WHERE EEID = :id "
            . "ORDER BY StartDate Desc"; 
        $dbh = DB_con();
        $qry = $dbh->prepare($SQL);
        $qry->bindParam(':id', $csv[1], PDO::PARAM_INT);
        $qry->execute();
        
        while($row = $qry->fetch(PDO::FETCH_NUM)){
         $id = $row[0]; 
        }
        
        unset($qry);
        unset($dbh);
        //preparing dates
        $startOfNewRec = substr($arr['timeStamp'], 0,-2) . '01';
        $endOfOldRec = strtotime('-1 day', strtotime($startOfNewRec));
        $startOfNewRec = date('Y-m-d', strtotime($startOfNewRec));
        $endOfOldRec = date('Y-m-d', strtotime($endOfOldRec));
        
        //get flag if MRU changed
        $changedDep = checkIfMRUChanged($csv[1], $csv[7]);
        
        //run DB update function
        updateJobDetails($csv, $id, $endOfOldRec, $startOfNewRec);
        
        //if MRU change flag is set to TRUE, remove EE from his old Team
        //plug EE to a new MRU afterwards
        if ('TRUE' == $changedDep) {
            removeFromTeam($csv[1], $endOfOldRec);
            addToNewMRU($csv[1], $csv[7]);
        }
        
     }
}


function checkEEdetails($csv){
//    function to check for employee details like name or email address
//    it searches for user in DB and checks if DB details are up to date. If
//    there's no match in DB, function updating record is called.
//    NOTE: records in main EE table are not time delimited, therefore there are
//    no record closing/opening operations happening
    $SQL = "SELECT Count(ID) FROM tHR_Employee WHERE ID = :eeID, LastName = :lName"
        . ", Email = :mail";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':eeID', $csv[1], PDO::PARAM_INT);
    $qry->bindParam(':lName', $csv[2], PDO::PARAM_STR);
    $qry->bindParam(':mail', $csv[4], PDO::PARAM_STR);
    $qry->execute();

    $result = 0;

    while($row = $qry->fetch(PDO::FETCH_NUM)){
        $result = $row[0];
    }
    //if result is different than 1, run update function to correct entry in DB
    if($result != 1){
        updateEEDetails($csv);
    }

    unset($qry);
    unset($dbh);
}

//Function which purpose is to determine if given employee has the same MRU in 
//BDW records compared to uploaded file. If the MRUs are different, it indicates 
//that employee has moved between departments and it has to be flagged for 
//reassigment to another MRU basket in tHR_OpsMRU
function checkIfMRUChanged($eeid, $mru){
    $SQL = "SELECT TOP 1 Project FROM tHR_JobDetails WHERE EEID = :id "
        . "ORDER BY StartDate DESC";
    $dbh = DB_con();
    $qry = $dbh->prepare($SQL);
    $qry->bindParam(':id', $eeid, PDO::PARAM_INT);
    $qry->execute();
    
    $result = 0;
    while($row = $qry->fetch(PDO::FETCH_NUM)){
        $result = $row[0];
    }
    
    if($result == $mru){
        return 'FALSE';
    } else {
        return 'TRUE';
    }
}

//=============================================================================
// >>>GTT UPLOAD PART
//=============================================================================


//stuff yet to come
?>
