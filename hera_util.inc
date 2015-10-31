<?php

// task types

define('TASK_TYPE_PUSH', 0);

// Utility functions

error_reporting(E_ALL);
ini_set('display_errors', true);
ini_set('display_startup_errors', true);

// parse config file, return as object
//
function get_config($file) {
    $home = getenv('HOME');
    $config_path = "$home/$file";
    return json_decode(file_get_contents($config_path));
}

function random_string() {
    return md5(uniqid(rand(), true));
}

?>
