#! /usr/bin/env php
<?php

// CLI script to create source/site/store records.
// Usage: db_init.php command args
//
// commands:
// source name
//      create a source with the given name
// store name capacity
//      create a store with the given name and capacity (in bytes)
// test_setup
//      create some records of each type for testing

require_once("hl_util.inc");
require_once("hl_db.inc");

function create_source($name) {
    $source = new StdClass;
    $source->name = $name;
    $source->authenticator = random_string();
    $source->create_time = time();
    echo "creating source $name; authenticator: $source->authenticator\n";
    return source_insert($source);
}

function create_store($name, $capacity) {
    $store = new StdClass;
    $store->name = $name;
    $store->create_time = time();
    $store->capacity = (double)$capacity;
    $store->used = 0;
    return store_insert($store);
}

function test_setup() {
    foreach (array('RTP', 'raw data') as $u) {
        if (!create_source($u)) {
            echo db_error()."\n";
        }
    }
    foreach (array('UC Berkeley', 'Penn', 'ASU') as $s) {
        if (!create_store($s, 1e12)) {
            echo db_error()."\n";
        }
    }
}

init_db();

switch ($argv[1]) {
case 'source': create_source($argv[2]); break;
case 'store': create_store($argv[2], $argv[3], $argv[4]); break;
case 'test_setup': test_setup(); break;
default: die("no such command\n");
}

?>
