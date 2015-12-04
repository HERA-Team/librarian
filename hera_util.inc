<?php

// Utility definitions and functions

// task types

define('TASK_TYPE_PUSH', 0);

// Utility functions

error_reporting(E_ALL);
ini_set('display_errors', true);
ini_set('display_startup_errors', true);

// parse client config file, return as object
//
function get_client_config() {
    $home = getenv('HOME');
    $config_path = "$home/.hl_client.cfg";
    if (!file_exists($config_path)) {
        die("No config file $config_path\n");
    }
    $c = json_decode(file_get_contents($config_path));
    if (!$c) {
        die("Can't parse config file $config_path\n");
    }
    return $c;
}

// parse server config file
//
function get_server_config() {
    $x = file_get_contents("hl_server.cfg");
    $c = json_decode($x);
    return $c;
}

function random_string() {
    return md5(uniqid(rand(), true));
}

?>
