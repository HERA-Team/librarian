<?php

// MySQL code for Librarian DB
//
// unless otherwise specified, functions return true on success

require_once("db_util.inc");

//// SOURCE ////

function source_insert($source) {
    global $link;
    $auth = $link->escape_string($source->authenticator);
    $query = "insert into source (name, authenticator, create_time) values ('$source->name', '$auth', $source->create_time)";
    return mysqli_query($link, $query);
}

function source_lookup_auth($auth) {
    global $link;
    $auth = $link->escape_string($auth);
    $query = "select * from source where authenticator='$auth'";
    $r = mysqli_query($link, $query);
    $source = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $source;
}

function source_lookup_id($id) {
    return lookup_id("source", $id);
}

function source_enum($clause=null) {
    return enum('source', $clause);
}

//// OBSERVATION ////

function observation_insert($obs) {
    global $link;
    $polarization = $link->escape_string($obs->polarization);
    $query = "insert into observation (id, source_id, julian_date, polarization, length) values ($obs->id, $obs->source_id, $obs->julian_date, '$polarization', $obs->length)";
    return mysqli_query($link, $query);
}

function observation_lookup_id($id) {
    return lookup_id("observation", $id);
}

function observation_enum($clause=null) {
    return enum('observation', $clause);
}

//// STORE ////

function store_insert($store) {
    global $link;
    $name = $link->escape_string($store->name);
    $rsync_prefix = $link->escape_string($store->rsync_prefix);
    $http_prefix = $link->escape_string($store->http_prefix);
    $path_prefix = $link->escape_string($store->path_prefix);
    $ssh_prefix = $link->escape_string($store->ssh_prefix);
    $query = "insert into store (name, create_time, capacity, used, rsync_prefix, http_prefix, path_prefix, ssh_prefix, unavailable) values ('$name', $store->create_time, $store->capacity, $store->used, '$rsync_prefix', '$http_prefix', '$path_prefix', '$ssh_prefix', 0)";
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

function store_update_all($id, $store) {
    global $link;
    $name = $link->escape_string($store->name);
    $rsync_prefix = $link->escape_string($store->rsync_prefix);
    $http_prefix = $link->escape_string($store->http_prefix);
    $path_prefix = $link->escape_string($store->path_prefix);
    $ssh_prefix = $link->escape_string($store->ssh_prefix);
    $unavailable = $store->unavailable?1:0;
    $query = "update store set name='$name', capacity=$store->capacity, used=$store->used, rsync_prefix='$rsync_prefix', http_prefix='$http_prefix', path_prefix='$path_prefix', ssh_prefix='$ssh_prefix', unavailable=$unavailable where id=$id";
    return mysqli_query($link, $query);
}

//// FILE ////

function file_insert($file) {
    global $link;
    $name = $link->escape_string($file->file_name);
    $type = $link->escape_string($file->type);
    $md5 = $link->escape_string($file->md5);
    $query = "insert into file (name, create_time, type, obs_id, source_id, store_id, size, md5, deleted, deleted_time) values ('$name', $file->create_time, '$type', $file->obs_id, $file->source_id, $file->store_id, $file->size, '$md5', 0, 0)";
    return mysqli_query($link, $query);
}

function file_lookup_name_store($name, $store_id) {
    global $link;
    $name = $link->escape_string($name);
    $query = "select * from file where name='$name' and store_id=$store_id";
    $r = mysqli_query($link, $query);
    $file = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $file;
}

function file_enum($clause=null) {
    return enum('file', $clause);
}

function file_count($clause=null) {
    return table_count('file', $clause);
}

///// TASK /////

function task_lookup_id($id) {
    return lookup_id("task", $id);
}

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
    $now = time();
    $file_name = $link->escape_string($task->file_name);
    $remote_site = $link->escape_string($task->remote_site_name);
    $remote_store = $link->escape_string($task->remote_store_name);
    $delete_when_done = $task->delete_when_done?1:0;
    $query = "insert into task (create_time, task_type, local_store_id, file_name, remote_site, remote_store, in_progress, delete_when_done, state, completed, completed_time, last_error, last_error_time) values ($now, $task->task_type, $task->local_store_id, '$file_name', '$remote_site', '$remote_store', 0, $delete_when_done, 0, 0, 0, '', 0)";
    return mysqli_query($link, $query);
}

function task_update_error($id, $msg) {
    global $link;
    $msg = $link->escape_string($msg);
    $now = time();
    $query = "update task set in_progress=0, last_error_time=$now, last_error='$msg' where id=$id";
    return mysqli_query($link, $query);

}

?>
