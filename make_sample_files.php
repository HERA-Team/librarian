#!/usr/bin/env php
<?php

// set up the M&C and Librarian DBs for testing.
// Create a bunch of observations and files.
// Do this using RPCs rather than direct DB access

require_once("hera_util.inc");
require_once("hl_rpc_client.php");
require_once("mc_rpc_client.php");

function test_setup() {
    global $test_store_names;
    $pols = array('xx', 'yy', 'xy', 'yx');

    for ($i=0; $i<50; $i++) {
        $julian_date = time() - rand(0, 100*86400);
        $polarization = $pols[rand(0, 3)];
        $length_days = .1*rand(0, 10);
//        $ret = mc_create_observation($julian_date, $polarization, $length_days);
//        if (!$ret->success) {
//            echo "mc_create_observation() error: $ret->message\n";
//            continue;
//        }
//        $obs_id = $ret->id;
        $obs_id = $i;
        $ret = create_observation(
            TEST_SITE_NAME, $obs_id, $julian_date, $polarization, $length_days
        );
        if (!$ret->success) {
            echo "create_observation() error: $ret->message\n";
            continue;
        }

        // for each observation, create a few files
        //

        for ($j=0; $j<5; $j++) {
            $f = "file_".$obs_id."_$j";
            $size = rand(1, 100)*1.e9;
            $store = $test_store_names[rand(0, 2)];
            $ret = create_file(
                TEST_SITE_NAME, $f, $obs_id, $size, random_string(), $store
            );
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
