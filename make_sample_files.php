#!/usr/bin/env php
<?php

// script to create a bunch of files and instances, for testing
// Do this using RPCs rather than direct DB access

require_once("hl_util.inc");
require_once("hl_rpc_client.php");

function test_setup() {
    $stores = array('UC Berkeley', 'Penn', 'ASU');
    for ($i=0; $i<100; $i++) {
        $julian_date = time() - rand(0, 100*86400);
        $polarization = "xy";
        $length_days = .5;
        $ret = create_observation($julian_date, $polarization, $length_days);
        if (!$ret->success) {
            echo "create_observation() error: $ret->message\n";
            continue;
        }
        $observation_id = $ret->id;
        $f = "file_$i";
        $size = rand(1, 100)*1.e9;
        $store = $stores[rand(0, 2)];
        $ret = create_file($f, $observation_id, $size, random_string(), $store);
        if ($ret->success) {
            echo "created file $f\n";
        } else {
            echo "create_file() error: $ret->message\n";
            continue;
        }
    }
}

test_setup();
?>
