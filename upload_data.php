<?php
$SafeFile = basename($_FILES['ufile']['name']); 
$uploaddir = "uploads/";

$path = $uploaddir.$SafeFile; 

if(move_uploaded_file($_FILES['ufile']['tmp_name'], $path)) {
    echo "The file ".  basename( $_FILES['uploadedfile']['name']). 
    " has been uploaded";
} else{
    echo "There was an error uploading the file, please try again!";
}

include "update_DB.php";

run_update();

?>