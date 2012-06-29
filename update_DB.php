<?php
include "../common/db_connection_PDO.php";

function run_update($inputData){
    switch($inputData['actionType']){
        case 'mig':
            migUpdate($inputData['filePath']);
            break;
    }
}


function migUpdate($path){
//  file handle  pointing to uploaded file
    $fh = fopen($path, "r");
    
//  parsing csv file - each row is imported as an array  
    while (($data = fgetcsv($fh)) !== FALSE) {
    print_r($data); //test operation
    echo("<br/>");
}
}
?>