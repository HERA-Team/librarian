<?php

// parse Librarian config file, return as object
//
function get_config() {
    $home = getenv('HOME');
    $config_path = "$home/.hera_librarian";
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
