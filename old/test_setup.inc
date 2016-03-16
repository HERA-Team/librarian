<?php

// values for test setup.  Edit as needed
//

// for UC Berkeley
//
if (gethostname() == "isaac") {
    define ('TEST_SITE_NAME', 'UCB_test');

    $store_0 = new StdClass;
    $store_0->name = 'Store 0';
    $store_0->capacity = 1e13;
    $store_0->used = 0;
    $store_0->rsync_prefix = "boincadm@isaac.ssl.berkeley.edu:hera/store_0";
    $store_0->http_prefix = "http://isaac.ssl.berkeley.edu/test/store_0";
    $store_0->path_prefix = "/mydisks/a/users/boincadm/hera/store_0";
    $store_0->ssh_prefix = "";
    $store_0->unavailable = false;

    // store 1 is remote
    $store_1 = new StdClass;
    $store_1->name = 'Store 1';
    $store_1->capacity = 1e13;
    $store_1->used = 0;
    $store_1->rsync_prefix = "davea@clarke.ssl.berkeley.edu:hera";
    $store_1->http_prefix = "";
    $store_1->path_prefix = "hera";
    $store_1->ssh_prefix = "davea@clarke.ssl.berkeley.edu";
    $store_1->unavailable = false;

    $test_stores = array($store_0, $store_1);
} else {
    // for Karoo
    //
    define ('TEST_SITE_NAME', 'Karoo');

    $store_0 = new StdClass;
    $store_0->name = 'Store 0';
    $store_0->capacity = 1e13;
    $store_0->used = 0;
    $store_0->rsync_prefix = "";
    $store_0->http_prefix = "http://isaac.ssl.berkeley.edu:8088/store_0";
    $store_0->path_prefix = "/home/obs/librarian/store_0";
    $store_0->ssh_prefix = "";
    $store_0->unavailable = false;

    $test_stores = array($store_0);
}

$test_source_names = array('RTP', 'Correlator');

?>

