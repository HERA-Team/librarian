<?php

error_reporting(E_ALL);
ini_set('display_errors', true);
ini_set('display_startup_errors', true);

// parse config file, return as object
//
function get_config($file) {
    $home = getenv('HOME');
    $config_path = "$home/$file";
    $lines = file($config_path);
    if (!$lines) {
        die("can't read $config_path\n");
    }
    $config = new StdClass;
    foreach ($lines as $line) {
        $x = explode(' ', $line);
        if (count($x) != 2) {
            die("bad config line $line\n");
        }

        $f = $x[0];
        $config->$f = trim($x[1]);
    }
    return $config;

}

function random_string() {
    return md5(uniqid(rand(), true));
}

?>
