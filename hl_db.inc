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

//// USER ////

function user_insert($user) {
    global $link;
    $auth = $link->escape_string($user->authenticator);
    $query = "insert into user (name, authenticator, create_time) values ('$user->name', '$auth', $user->create_time)";
    return mysqli_query($link, $query);
}

function user_lookup_auth($auth) {
    global $link;
    $auth = $link->escape_string($auth);
    $query = "select * from user where authenticator='$auth'";
    $r = mysqli_query($link, $query);
    $user = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $user;
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

//// FILE ////

function file_insert($file) {
    global $link;
    $md5 = $link->escape_string($file->md5);
    $name = $link->escape_string($file->name);
    $query = "insert into file (name, create_time, size, user_id, md5) values ('$name', $file->create_time, $file->size, $file->user_id, '$md5')";
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

function file_enum($clause) {
    global $link;
    if (!$clause) $clause = 'TRUE';
    $query = "select * from file where $clause";
    $r = mysqli_query($link, $query);
    $files = array();
    while ($f = mysqli_fetch_object($r)) {
        $files[] = $f;
    }
    mysqli_free_result($r);
    return $files;
}

//// FILE_INSTANCE ////

function file_instance_insert($fi) {
    global $link;
    $query = "insert into file_instance (file_id, store_id, create_time, user_id) values ($fi->file_id, $fi->store_id, $fi->create_time, $fi->user_id)";
    return mysqli_query($link, $query);
}

?>
