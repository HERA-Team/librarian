<?php

// Librarian DB access

$link = null;
$db_error = '';

// connect to a DB with the given parameters
//
function init_db($config) {
    global $link;
    $link = @pg_connect(
      "host=$config->db_host user=$config->db_user password=$config->db_passwd dbname=$config->db_name"
    );
    return $link;
}

// get string describing last error
//
function db_error() {
    global $link;
    return pg_last_error($link);
}

function db_escape($text) {
    global $link;
    return pg_escape_string($link, $text);
}

function enum_general($query) {
    global $link;
    $r = pg_query($link, $query);
    $items = array();
    while ($f = pg_fetch_object($r)) {
        $items[] = $f;
    }
    pg_free_result($r);
    return $items;
}

// enumerate a table
//
function enum($table, $clause=null, $extrasel=null) {
    if (!$clause) $clause = 'TRUE';
    if (!$extrasel) {
        $extrasel = "";
    } else {
        $extrasel = ", " . $extrasel;
    }
    $query = "select *$extrasel from $table where $clause";
    return enum_general($query);
}

function table_count($table, $clause) {
    global $link;
    if (!$clause) $clause = 'TRUE';
    $query = "select count(*) as total from $table where $clause";
    $x = enum_general($query);
    if(count($x) == 0)
        return 0;
    return $x[0]->total;
}

// look up record from table with given ID
//
function lookup_id($table, $id) {
    global $link;
    $query = "select * from $table where id=$id";
    $x = enum_general($query);
    if(count($x) == 0)
        return false;
    return $x[0];
}

// look up obsid from the observation table (specific instance of lookup_id)
//
function lookup_obsid($table, $id) {
    global $link;
    $query = "select * from $table where obsid=$id";
    $x = enum_general($query);
    if(count($x) == 0)
        return false;
    return $x[0];
}

// return # of rows affected by last query
//
function affected_rows() {
    global $link;
    return pg_affected_rows($link);
}

?>
