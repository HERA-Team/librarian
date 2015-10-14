#!/usr/bin/env php
<?php

// Create a bunch of observations and files, for testing.
// Do this using RPCs rather than direct DB access

require_once("hera_util.inc");
require_once("hl_rpc_client.php");
require_once("mc_rpc_client.php");

function test_setup() {
    $stores = array('UC Berkeley', 'Penn', 'ASU');
    $pols = array('xx', 'yy', 'xy', 'yx');
    for ($i=0; $i<50; $i++) {
        $julian_date = time() - rand(0, 100*86400);
        $polarization = $pols[rand(0, 3)];
        $length_days = .1*rand(0, 10);
        $ret = mc_create_observation($julian_date, $polarization, $length_days);
        if (!$ret->success) {
            echo "mc_create_observation() error: $ret->message\n";
            continue;
        }
        $observation_id = $ret->id;
        $ret = hl_create_observation(
            $observation_id, $julian_date, $polarization, $length_days
        );
        if (!$ret->success) {
            echo "hl_create_observation() error: $ret->message\n";
            continue;
        }

        // for each observation, create a few files
        //

        for ($j=0; $j<5; $j++) {
            $f = "file_".$observation_id."_$j";
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
}

test_setup();
?>
