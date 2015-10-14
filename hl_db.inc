<?php

// MySQL code goes here

$link = null;
$db_error = '';
define('DB_NAME', 'hera');

function init_db() {
    global $link;
    $link = @mysqli_connect('', '', '', DB_NAME);
    return $link;
}

function insert_id() {
    global $link;
    return mysqli_insert_id($link);
}

function db_error() {
    global $link;
    return mysqli_error($link);
}

function enum($table, $clause=null) {
    global $link;
    if (!$clause) $clause = 'TRUE';
    $query = "select * from $table where $clause";
    $r = mysqli_query($link, $query);
    $items = array();
    while ($f = mysqli_fetch_object($r)) {
        $items[] = $f;
    }
    mysqli_free_result($r);
    return $items;
}

function lookup_id($table, $id) {
    global $link;
    $query = "select * from $table where id=$id";
    $r = mysqli_query($link, $query);
    $source = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $source;
}


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

function source_enum() {
    return enum('source');
}

//// OBSERVATION ////

function observation_insert($obs) {
    global $link;
    $polarization = $link->escape_string($obs->polarization);
    $query = "insert into observation (source_id, julian_date, polarization, length_days) values ($obs->source_id, $obs->julian_date, '$polarization', $obs->length_days)";
    return mysqli_query($link, $query);
}

function observation_lookup_id($id) {
    return lookup_id("observation", $id);
}

//// STORE ////

function store_insert($store) {
    global $link;
    $name = $link->escape_string($store->name);
    $query = "insert into store (name, create_time, capacity, used) values ('$name', $store->create_time, $store->capacity, $store->used)";
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
    $query = "insert into file (name, create_time, observation_id, source_id, store_id, size, md5) values ('$name', $file->create_time, $file->observation_id, $file->source_id, $file->store_id, $file->size, '$md5')";
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

?>
