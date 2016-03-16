#! /usr/bin/env php
<?php

require_once("hl_rpc_client.php");

$ret = create_copy_task(
    TASK_TYPE_PUSH,
    "Karoo", "Store 0", "file_1_0", "UCB_test", "Store 0", false
);

if (!$ret->success) {
    echo "error: $ret->message\n";
}

?>
