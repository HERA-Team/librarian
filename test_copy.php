#! /usr/bin/env php
<?php

require_once("hl_rpc_client.php");

$ret = create_copy_task(
    TASK_TYPE_PUSH,
    "test_site", "Store 0", "file_1_0", "test_site", "Store 1", false
);

if (!$ret->success) {
    echo "error: $ret->message\n";
}

?>
