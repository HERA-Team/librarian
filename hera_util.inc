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

function get_file_info($store, $file_name) {
    if ($store->ssh_host) {
        $cmd = "ssh $store->ssh_prefix md5sum $store->path_prefix/$file_name";
        $out = system($cmd);
        $x = explode($out, " ");
        $md5 = $x[0];
        $cmd = "ssh $store->ssh_prefix wc -c $store->path_prefix/$file_name";
        $out = system($cmd);
        $x = explode($out, " ");
        $size = $x[0];
    } else {
        $path = "$store->path_prefix/$file_name";
        $size = filesize($path);
        $md5 = md5_file($path);
    }
    return array($size, $md5);
}

?>
