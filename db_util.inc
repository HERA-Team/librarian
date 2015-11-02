<?php

// MySQL code for MC and Librarian DBs

$link = null;
$db_error = '';
define('LIBRARIAN_DB_NAME', 'hera_lib');
define('MC_DB_NAME', 'hera_mc');

// connect to a DB of the given name
//
function init_db($db_name) {
    global $link;
    $link = @mysqli_connect('', '', '', $db_name);
    if (!$link) {
        echo "can't connect to $db_name\n";
    }
    return $link;
}

// get ID of last insert
//
function insert_id() {
    global $link;
    return mysqli_insert_id($link);
}

// get string describing last error
//
function db_error() {
    global $link;
    return mysqli_error($link);
}

// enumerate a table
//
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

function table_count($table, $clause) {
    global $link;
    if (!$clause) $clause = 'TRUE';
    $query = "select count(*) as total from $table where $clause";
    $r = mysqli_query($link, $query);
    $x = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $x->total;
}

// look up record from table with given ID
//
function lookup_id($table, $id) {
    global $link;
    $query = "select * from $table where id=$id";
    $r = mysqli_query($link, $query);
    $source = mysqli_fetch_object($r);
    mysqli_free_result($r);
    return $source;
}

// return # of rows affected by last query
//
function affected_rows() {
    global $link;
    return $link->affected_rows;
}

// Functions for tables common to M&C and HL databases

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

?>
