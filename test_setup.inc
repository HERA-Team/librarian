<?php

// names for testing
//

define ('TEST_SITE_NAME', 'test_site');

$store0 = new StdClass;
$store0->name = 'Store 0';
$store0->capacity = 1e13;
$store0->used = 0;
$store0->rsync_prefix = "boincadm@isaac.ssl.berkeley.edu/hera/store0";
$store0->http_prefix = "http://isaac.ssl.berkeley.edu/test/store0";
$store0->path = "/mydisks/a/users/boincadm/hera/store0";

$store1 = new StdClass;
$store1->name = 'Store 1';
$store1->capacity = 1e13;
$store1->used = 0;
$store1->rsync_prefix = "boincadm@isaac.ssl.berkeley.edu/hera/store1";
$store1->http_prefix = "http://isaac.ssl.berkeley.edu/test/store1";
$store1->path = "/mydisks/a/users/boincadm/hera/store1";

$test_stores = array($store0, $store1);

$test_source_names = array('RTP', 'Correlator');

?>

