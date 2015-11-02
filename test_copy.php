#! /usr/bin/env php
<?php

require_once("hl_rpc_client.php");

$ret = create_task(
    TASK_TYPE_PUSH,
    "test_site", "Store 0", "foo", "test_site", "Store 1", false
);

if (!$ret->success) {
    echo "error: $ret->message\n";
}

?>
