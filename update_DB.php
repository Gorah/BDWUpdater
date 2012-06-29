<?php
include "../common/db_connection_PDO.php";

function run_update($inputData){
    switch($inputData['actionType']){
        case 'mig':
            $funct_name = 'migUpdate';
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


function migUpdate($array){
//    receives a row of data in form of an array and processes it against DB
}
?>