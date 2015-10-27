#! /usr/bin/env php
<?php

// copier.php --task_id id
//
// start (or restart) a file copy operation.
//
// task.state tells us how much of the operating we've already done.
// values of task.state:
// 0 initial
// 1 completed rsync
// 2 registered file with remote Librarian
// 3 deleted local copy

require_once("hl_db.inc");
require_once("hl_rpc_client.php");

// do the actual file transfer with rsync
//
function do_rsync($task, $local_store, $file) {
    echo "looking up remote store\n";
    $ret = lookup_store($task->remote_site, $task->remote_store);
    if (!$ret->success) {
        echo "couldn't find remote store $task->remote_store\n";
        task_update_error($task->id, $ret->message);
        exit(1);
    }
    $remote_store = $ret->store;
    $dest = $remote_store->rsync_prefix.'/'.$file->name;
    $path = $local_store->path.'/'.$file->name;
    $cmd = "rsync $path $dest 2>&1";
    //echo "copier: execing $cmd\n";
    exec($cmd, $output, $status);
    if ($status) {
        task_update_error($task->id, implode("\n", $output));
        exit(1);
    }
    echo "rsync finished\n";
}

// register file with remote Librarian
//
function do_remote_register($task, $file) {
    $ret = create_file(
        $task->remote_site, $file->name, $file->obs_id, $file->size,
        $file->md5, $task->remote_store
    );
    if (!$ret->success) {
        task_update_error($task->id, $ret->message);
        exit(1);
    }
}

function do_task($task_id) {
    $task = task_lookup_id($task_id);
    if (!$task) {
        die("no such task: $task_id\n");
    }
    $local_store = store_lookup_name($task->local_store);
    if (!$local_store) {
        die("no such store: $task->local_store\n");
    }
    $file = file_lookup_name_store($task->file_name, $local_store->id);
    if (!$file) {
        die("no such file: $task->file_name");
    }
    if ($task->state == 0) {
        do_rsync($task, $local_store, $file);
        $ret = task_update($task->id, "state=1");
        if (!$ret) {
            task_update_error($task->id, "task_update() failed");
        }
        $task->state = 1;
    }
    if ($task->state < 2) {
        do_remote_register($task, $file);
        $ret = task_update($task->id, "state=2");
        if (!$ret) {
            task_update_error($task->id, "task_update() failed");
        }
        $task->state = 2;
    }
    if ($task->delete_when_done) {
        if (file_exists($file->path)) {
            $ret = unlink($file->path);
            if (!$ret) {
                task_update_error($task->id, "unlink($file->path) failed");
                exit(1);
            }
        }
        $now = time();
        $ret = file_update($file->id, "deleted=1 and delete_time=$now");
        if (!$ret) {
            task_update_error($task->id, "file_update() failed");
            exit(1);
        }
    }

    // all done.  update task record
    //
    $now = time();
    task_update($task->id, "completed=1, completed_time=$now");
}

$task_id = 0;
for ($i=1; $i<$argc; $i++) {
    switch ($argv[$i]) {
    case '--task_id':
        $task_id = (int)$argv[++$i];
        break;
    default:
        die("copier.php: bad arg: ".$argv[$i]."\n");
    }
}

if (!$task_id) {
    die ("no task ID\n");
}

init_db(LIBRARIAN_DB_NAME);
do_task($task_id);

?>
