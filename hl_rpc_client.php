<?php

// MySQL binding of RPC interface to create files / file instances
//
// The functions return a $reply object:
// $reply->success: 1 if success, 0 if failure
// $reply->message: if failure, error message

require_once("hl_util.inc");

$config = get_config();

function do_http_post($req) {
    global $config;
    $server = $config->server;
    $url = "$server/hl_rpc_handler.php";
    $req->authenticator = $config->authenticator;
    $req_json = json_encode($req);
    $post_args = array();
    $post_args['request'] = $req_json;

    $ch = curl_init($url);
    curl_setopt($ch, CURLOPT_POST, 1);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $post_args);
    $reply_json = curl_exec($ch);
    curl_close($ch);
    $ret = json_decode($reply_json);
    if ($ret) return $ret;
    $ret = new StdClass;
    $ret->success = 0;
    $ret->message = "can't parse JSON reply: $reply_json";
    return $ret;
}

function create_observation($julian_date, $polarization, $length_days) {
    $req = new StdClass;
    $req->operation = 'create_observation';
    $req->julian_date = $julian_date;
    $req->polarization = $polarization;
    $req->length_days = $length_days;
    return do_http_post($req);
}

function create_file($name, $observation_id, $size, $md5, $store_name) {
    $req = new StdClass;
    $req->operation = 'create_file';
    $req->name = $name;
    $req->observation_id = $observation_id;
    $req->size = $size;
    $req->md5 = $md5;
    $req->store_name = $store_name;
    return do_http_post($req);
}

?>
