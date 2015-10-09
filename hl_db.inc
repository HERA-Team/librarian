<?php

// All MySQL code goes here

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

//// USER ////

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
    global $link;
    $query = "select * from source where id=$id";
    $r = mysqli_query($link, $query);
    $source = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $source;
}

function source_enum() {
    return enum('source');
}

//// SITE ////

function site_insert($site) {
    global $link;
    $name = $link->escape_string($site->name);
    $query = "insert into site (name, create_time) values ('$name', $site->create_time)";
    return mysqli_query($link, $query);
}

function site_lookup_name($name) {
    global $link;
    $name = $link->escape_string($name);
    $query = "select * from site where name='$name'";
    $r = mysqli_query($link, $query);
    $site = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $site;
}

function site_enum() {
    return enum('site');
}

//// STORE ////

function store_insert($store) {
    global $link;
    $name = $link->escape_string($store->name);
    $query = "insert into store (site_id, name, create_time, capacity, used) values ($store->site_id, '$name', $store->create_time, $store->capacity, $store->used)";
    return mysqli_query($link, $query);
}

function store_lookup_name($site_id, $name) {
    global $link;
    $name = $link->escape_string($name);
    $query = "select * from store where site_id=$site_id and name='$name'";
    $r = mysqli_query($link, $query);
    $store = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $store;
}

function store_enum($clause='') {
    return enum('store', $clause);
}

//// FILE ////

function file_insert($file) {
    global $link;
    $md5 = $link->escape_string($file->md5);
    $name = $link->escape_string($file->name);
    $query = "insert into file (name, create_time, size, source_id, md5) values ('$name', $file->create_time, $file->size, $file->source_id, '$md5')";
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

//// FILE_INSTANCE ////

function file_instance_insert($fi) {
    global $link;
    $query = "insert into file_instance (file_id, store_id, create_time, source_id) values ($fi->file_id, $fi->store_id, $fi->create_time, $fi->source_id)";
    return mysqli_query($link, $query);
}

function file_instance_enum($clause=null) {
    return enum('file_instance', $clause);
}

?>
