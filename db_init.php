#! /usr/bin/env php
<?php

// CLI script to create Librarian and MC records.
// Usage: db_init.php command args
//
// commands:
// hl_source name
//      create a Librarian source with the given name
// mc_source name
//      create a MC source with the given name
// store name capacity
//      create a store with the given name and capacity (in bytes)
// test_setup
//      create records for testing

require_once("hera_util.inc");
require_once("hl_db.inc");

function create_source($name, $subsystem) {
    $source = new StdClass;
    $source->name = $name;
    $source->authenticator = random_string();
    $source->create_time = time();
    echo "creating $subsystem source $name; authenticator: $source->authenticator\n";
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

function mc_setup() {
    init_db(MC_DB_NAME);
    foreach (array('RTP', 'raw data') as $u) {
        if (!create_source($u, 'M&C')) {
            echo db_error()."\n";
        }
    }
}

function hl_setup() {
    init_db(LIBRARIAN_DB_NAME);
    foreach (array('RTP', 'raw data') as $u) {
        if (!create_source($u, 'Librarian')) {
            echo db_error()."\n";
        }
    }
    foreach (array('UC Berkeley', 'Penn', 'ASU') as $s) {
        if (!create_store($s, 1e12)) {
            echo db_error()."\n";
        }
    }
}


switch ($argv[1]) {
case 'hl_source':
    init_db(LIBRARIAN_DB_NAME);
    create_source($argv[2]);
    break;
case 'mc_source':
    init_db(MC_DB_NAME);
    create_source($argv[2]);
    break;
case 'store':
    init_db(LIBRARIAN_DB_NAME);
    create_store($argv[2], $argv[3], $argv[4]);
    break;
case 'test_setup':
    mc_setup();
    hl_setup();
    break;
default: die("no such command\n");
}

?>
