<?php

// MySQL code for Librarian DB

require_once("db_util.inc");

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
