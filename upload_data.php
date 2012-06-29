<?php

//taking file name from uploaded file and creating file path&name
$SafeFile = basename($_FILES['ufile']['name']); 
$uploaddir = "uploads/";

$path = $uploaddir.$SafeFile; 

//copying file to upload location
if(move_uploaded_file($_FILES['ufile']['tmp_name'], $path)) {
    echo "The file ".  basename( $_FILES['uploadedfile']['name']). 
    " has been uploaded";
} else{
    echo "There was an error uploading the file, please try again!";
}

//including file with following update scripts and calling function out
include "update_DB.php";

$data = array('filePath' => $path,
              'actionType' => $_POST['utype']);

run_update($data);

?>