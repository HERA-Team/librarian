<?php

// names for testing
//

define ('TEST_SITE_NAME', 'test_site');

$store_0 = new StdClass;
$store_0->name = 'Store 0';
$store_0->capacity = 1e13;
$store_0->used = 0;
$store_0->rsync_prefix = "boincadm@isaac.ssl.berkeley.edu/hera/store_0";
$store_0->http_prefix = "http://isaac.ssl.berkeley.edu/test/store_0";
$store_0->path = "/mydisks/a/users/boincadm/hera/store_0";
$store_0->ssh_host = "";
$store_0->unavailable = false;

$store_1 = new StdClass;
$store_1->name = 'Store 1';
$store_1->capacity = 1e13;
$store_1->used = 0;
$store_1->rsync_prefix = "boincadm@isaac.ssl.berkeley.edu/hera/store_1";
$store_1->http_prefix = "http://isaac.ssl.berkeley.edu/test/store_1";
$store_1->path = "/mydisks/a/users/boincadm/hera/store_1";
$store_1->ssh_host = "";
$store_1->unavailable = false;

$test_stores = array($store_0, $store_1);

$test_source_names = array('RTP', 'Correlator');

?>

