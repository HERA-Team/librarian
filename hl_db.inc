<?php

// MySQL code for Librarian DB
//
// unless otherwise specified, functions return true on success

require_once("db_util.inc");

//// SOURCE ////

function source_insert($source) {
    global $link;
    $auth = pg_escape_string($link, $source->authenticator);
    $query = "insert into source (name, authenticator, create_time) values ('$source->name', '$auth', to_timestamp($source->create_time)) returning id";
    return pg_query($link, $query);
}

function source_lookup_auth($auth) {
    global $link;
    $auth = pg_escape_string($link, $auth);
    $x = enum ("source", "authenticator='$auth'", "extract(epoch from create_time) as create_time");
    if(count($x) == 0)
        return false;
    return $x[0];
}

function source_lookup_id($id) {
    return lookup_id("source", $id);
}

function source_enum($clause=null) {
    return enum('source', $clause, "extract(epoch from create_time) as create_time");
}

//// OBSERVATION ////

function observation_insert($obs) {
    global $link;
    $polarization = pg_escape_string($link, $obs->polarization);
    $query = "insert into observation (id, source_id, julian_date, polarization, length) values ($obs->id, $obs->source_id, $obs->julian_date, '$polarization', $obs->length) returning id";
    return pg_query($link, $query);
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
    $name = pg_escape_string($link, $store->name);
    $rsync_prefix = pg_escape_string($link, $store->rsync_prefix);
    $http_prefix = pg_escape_string($link, $store->http_prefix);
    $path_prefix = pg_escape_string($link, $store->path_prefix);
    $ssh_prefix = pg_escape_string($link, $store->ssh_prefix);
    $query = "insert into store (name, create_time, capacity, used, rsync_prefix, http_prefix, path_prefix, ssh_prefix, unavailable) values ('$name', to_timestamp($store->create_time), $store->capacity, $store->used, '$rsync_prefix', '$http_prefix', '$path_prefix', '$ssh_prefix', 0) returning id";
    return pg_query($link, $query);
}

function store_lookup_name($name) {
    global $link;
    $name = pg_escape_string($link, $name);
    $x = enum ("store", "name='$name'", "extract(epoch from create_time) as create_time");
    if(count($x) == 0)
        return false;
    return $x[0];
}

function store_enum($clause='') {
    return enum('store', $clause, "extract(epoch from create_time) as create_time");
}

function store_lookup_id($id) {
    return lookup_id("store", $id);
}

function store_update($id, $clause) {
    global $link;
    $query = "update store set $clause where id=$id";
    return pg_query($link, $query);
}

function store_update_all($id, $store) {
    global $link;
    $name = pg_escape_string($link, $store->name);
    $rsync_prefix = pg_escape_string($link, $store->rsync_prefix);
    $http_prefix = pg_escape_string($link, $store->http_prefix);
    $path_prefix = pg_escape_string($link, $store->path_prefix);
    $ssh_prefix = pg_escape_string($link, $store->ssh_prefix);
    $unavailable = $store->unavailable?1:0;
    $query = "update store set name='$name', capacity=$store->capacity, used=$store->used, rsync_prefix='$rsync_prefix', http_prefix='$http_prefix', path_prefix='$path_prefix', ssh_prefix='$ssh_prefix', unavailable=$unavailable where id=$id";
    return pg_query($link, $query);
}

//// FILE ////

function file_insert($file) {
    global $link;
    $name = pg_escape_string($link, $file->file_name);
    $type = pg_escape_string($link, $file->type);
    $md5 = pg_escape_string($link, $file->md5);
    $query = "insert into file (name, create_time, type, obs_id, source_id, store_id, size, md5, deleted, deleted_time) values ('$name', to_timestamp($file->create_time), '$type', $file->obs_id, $file->source_id, $file->store_id, $file->size, '$md5', 0, to_timestamp(0)) returning id";
    return pg_query($link, $query);
}

function file_lookup_name_store($name, $store_id) {
    global $link;
    $name = pg_escape_string($link, $name);
    $x = enum ("file", "name='$name' and store_id=$store_id",
               "extract(epoch from create_time) as create_time, extract(epoch from deleted_time) as deleted_time");
    if(count($x) == 0)
        return false;
    return $x[0];
}

function file_enum($clause=null) {
    return enum('file', $clause,
      "extract(epoch from create_time) as create_time, extract(epoch from deleted_time) as deleted_time");
}

function file_count($clause=null) {
    return table_count('file', $clause);
}

function file_update($id, $clause) {
    global $link;
    $query = "update file set $clause where id=$id";
    return pg_query($link, $query);
}

///// HISTORY /////

function history_insert($hist) {
    // Insert a new record into the 'history' table. $hist is an object
    // containing at least the following keys:
    //
    //  file_id -- the ID number of the file row that this history item refers to
    //  type    -- string giving the event type
    //  payload -- string of event data; interpretation depends on 'type'
    //
    // We do not verify that file_id is valid, except to the extent that the
    // database will yell at us for breaking a foreign key constraint if it's
    // wrong.

    global $link;
    $now = time();

    $type = db_escape($hist->type);
    $payload = db_escape($hist->payload);

    $query = <<<SQL
INSERT INTO history (create_time, file_id, type, payload)
VALUES (to_timestamp($now), $hist->file_id, '$type', '$payload')
RETURNING id
SQL;
    return pg_query($link, $query);
}

function history_enum($clause) {
    return enum ('history', $clause,
                 'extract(epoch from create_time) as create_time');
}


///// TASK /////

function task_lookup_id($id) {
    return lookup_id("task", $id);
}

function task_enum($clause=null) {
    return enum('task', $clause,
      "extract(epoch from create_time) as create_time, extract(epoch from completed_time) as completed_time,
       extract(epoch from last_error_time) as last_error_time");
}

function task_update_all($set_clause, $where_clause) {
    global $link;
    $query = "update task set $set_clause where $where_clause";
    return pg_query($link, $query);
}

function task_update($id, $clause) {
    global $link;
    $query = "update task set $clause where id=$id";
    return pg_query($link, $query);
}

function task_insert($task) {
    global $link;
    $now = time();
    $file_name = pg_escape_string($link, $task->file_name);
    $remote_site = pg_escape_string($link, $task->remote_site_name);
    $remote_store = pg_escape_string($link, $task->remote_store_name);
    $delete_when_done = $task->delete_when_done?1:0;
    $query = "insert into task (create_time, task_type, local_store_id, file_name, remote_site, remote_store, in_progress, delete_when_done, state, completed, completed_time, last_error, last_error_time) values (to_timestamp($now), $task->task_type, $task->local_store_id, '$file_name', '$remote_site', '$remote_store', 0, $delete_when_done, 0, 0, to_timestamp(0), '', to_timestamp(0)) returning id";
    return pg_query($link, $query);
}

function task_update_error($id, $msg) {
    global $link;
    $msg = pg_escape_string($link, substr($msg, 0, 254));
    $now = time();
    $query = "update task set in_progress=0, last_error_time=to_timestamp($now), last_error='$msg' where id=$id";
    return pg_query($link, $query);
}

?>
