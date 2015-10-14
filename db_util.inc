<?php

// MySQL code for MC and Librarian DBs

$link = null;
$db_error = '';
define('LIBRARIAN_DB_NAME', 'hera_lib');
define('MC_DB_NAME', 'hera_mc');

function init_db($db_name) {
    global $link;
    $link = @mysqli_connect('', '', '', $db_name);
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

function observation_insert_hl($obs) {
    global $link;
    $polarization = $link->escape_string($obs->polarization);
    $query = "insert into observation (id, source_id, julian_date, polarization, length_days) values ($obs->id, $obs->source_id, $obs->julian_date, '$polarization', $obs->length_days)";
    return mysqli_query($link, $query);
}

function observation_insert_mc($obs) {
    global $link;
    $polarization = $link->escape_string($obs->polarization);
    $query = "insert into observation (source_id, julian_date, polarization, length_days) values ($obs->source_id, $obs->julian_date, '$polarization', $obs->length_days)";
    return mysqli_query($link, $query);
}

function observation_lookup_id($id) {
    return lookup_id("observation", $id);
}

?>
