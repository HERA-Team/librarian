#!/usr/bin/env php
<?php

// script to create a bunch of files and instances, for testing
// Do this using RPCs rather than direct DB access

error_reporting(E_ALL);
ini_set('display_errors', true);
ini_set('display_startup_errors', true);

require_once("hl_util.inc");
require_once("hl_rpc_client.php");

function test_setup() {
    $sites = array('UC Berkeley', 'Penn', 'ASU');
    $stores = array('Luster', 'RAID Box');
    for ($i=0; $i<100; $i++) {
        $f = "file_$i";
        $size = rand(1, 100)*1.e9;
        $ret = create_file($f, $size, random_string());
        if ($ret->success) {
            echo "created file $f\n";
        } else {
            echo "create_file() error: $ret->message\n";
            continue;
        }
        for ($j=0; $j<3; $j++) {
            $site = $sites[rand(0,2)];
            $store = $stores[rand(0,1)];
            $ret = create_file_instance($f, $site, $store);
            if ($ret->success) {
                echo "  created instance on $site $store\n";
            } else {
                echo "create_file_instance() error: $ret->message\n";
            }
        }
    }
}

test_setup();
?>
