#! /usr/bin/env php
<?php

// CLI script to create user/site/store records.
// Usage:
//
// db_init.php user name
// db_init.php site name
// db_init.php store site_name name capacity

require_once("hl_db.inc");

function create_user($name) {
    $user = new StdClass;
    $user->name = $name;
    $user->authenticator = random_string();
    $user->create_time = time();
    user_insert($user);
}

function create_site($name) {
    $site = new StdClass;
    $site->name = $name;
    $site->create_time = time();
    site_insert($site);
}

function create_store($site_name, $name, $capacity) {
    $site = site_lookup_name($site_name);
    $store = new StdClass;
    $store->site_id = $site->id;
    $store->name = $name;
    $store->create_time = time();
    $store->capacity = (double)$capacity;
    $store->used = 0;
    store_insert($store);
}

init_db();

switch ($argv[1]) {
case 'user': create_user($argv[2]); break;
case 'site': create_site($argv[2]); break;
case 'store': create_store($argv[2], $argv[3], $argv[4]); break;
default: die("no such command\n");
}

?>
