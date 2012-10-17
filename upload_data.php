<?php

//this script takes csv file and action type from html form which called it.
//Script does copy the file to /uploads folder, where the file is accessed by 
//the updater that's being called with run_update(); function.
//after updaters job is done, script removes the file from the folder cleaning 
//up after itself.

//taking file name from uploaded file and creating file path&name
$SafeFile = basename($_FILES['ufile']['name']); 
$uploaddir = "uploads/";

$path = $uploaddir.$SafeFile; 

//copying file to upload location
if(move_uploaded_file($_FILES['ufile']['tmp_name'], $path)) {
    echo "The file ".  $SafeFile. " has been uploaded";
} else{
    echo "There was an error uploading the file, please try again!";
}

//including file with following update scripts and calling function out
include "update_DB.php";

$data = array('filePath' => $path,
              'actionType' => $_POST['utype'],
               'timeStamp' => $_POST['timeStamp']);

run_update($data);

//removing the file from the server
unlink($path);

?>