<?php

// MySQL code for Librarian DB
//
// unless otherwise specified, functions return true on success

require_once("db_util.inc");

//// STORE ////

function store_insert($store) {
    global $link;
    $name = $link->escape_string($store->name);
    $rsync_base = $link->escape_string($store->rsync_base);
    $query = "insert into store (name, create_time, capacity, used, rsync_base) values ('$name', $store->create_time, $store->capacity, $store->used, '$rsync_base')";
    return mysqli_query($link, $query);
}

function store_lookup_name($name) {
    global $link;
    $name = $link->escape_string($name);
    $query = "select * from store where name='$name'";
    $r = mysqli_query($link, $query);
    $store = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $store;
}

function store_enum($clause='') {
    return enum('store', $clause);
}

function store_lookup_id($id) {
    return lookup_id("store", $id);
}

function store_update($id, $clause) {
    global $link;
    $query = "update store set $clause where id=$id";
    return mysqli_query($link, $query);
}

//// FILE ////

function file_insert($file) {
    global $link;
    $md5 = $link->escape_string($file->md5);
    $name = $link->escape_string($file->name);
    $query = "insert into file (name, create_time, obs_id, source_id, store_id, size, md5) values ('$name', $file->create_time, $file->obs_id, $file->source_id, $file->store_id, $file->size, '$md5')";
    return mysqli_query($link, $query);
}

function file_lookup_name($name) {
    global $link;
    $name = $link->escape_string($name);
    $query = "select * from file where name='$name'";
    $r = mysqli_query($link, $query);
    $file = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $file;
}

function file_enum($clause=null) {
    return enum('file', $clause);
}

///// TASK /////

function task_enum($clause=null) {
    return enum('task', $clause);
}

function task_update_all($set_clause, $where_clause) {
    global $link;
    $query = "update task set $set_clause where $where_clause";
    return mysqli_query($link, $query);
}

function task_update($id, $clause) {
    global $link;
    $query = "update task set $clause where id=$id";
    return mysqli_query($link, $query);
}

function task_insert($task) {
    global $link;
    $local_name = $link->escape_string($file->local_name);
    $remote_name = $link->escape_string($file->remote_name);
    $query = "insert into task (create_time, local_name, remote_name, start_time, pid, in_progress, completed, last_error) values ($file->create_time, '$file->local_name', '$file->remote_name', 0, 0, 0, 0, '')";
    return mysqli_query($link, $query);
}

function task_update_error($id, $msg) {
    global $link;
    $msg = $link->escape_string($msg);
    $now = time();
    $query = "update task set last_error_time=$now, last_error='$msg' where id=$id";
    return mysqli_query($link, $query);

}

?>
