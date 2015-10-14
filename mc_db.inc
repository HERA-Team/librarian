<?php
require_once("db_util.inc");

function status_insert($status) {
    global $link;
    $still_host = $link->escape_string($status->still_host);
    $still_path = $link->escape_string($status->still_path);
    $output_host = $link->escape_string($status->output_host);
    $output_path = $link->escape_string($status->output_path);
    $query = "insert into status (observation_id, status, current_pid, still_host, still_path, output_host, output_path, source_id) values ($status->observation_id, $status->status, $status->current_pid, '$still_host', '$still_path', '$output_host', '$output_path')";
    return mysqli_query($link, $query);
}

?>
