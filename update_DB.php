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
    
}

function update_EE($csv){
    
}
?>