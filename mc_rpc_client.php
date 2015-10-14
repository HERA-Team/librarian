<?php

// PHP binding of M&C RPC interface
//
// The functions return a $reply object:
// $reply->success: 1 if success, 0 if failure
// $reply->message: if failure, error message
// $reply->id: for creation RPC, the DB ID of the created item

require_once("hera_util.inc");

$mc_config = get_config('.hera_mc');

function mc_do_http_post($req) {
    global $mc_config;
    $server = $mc_config->server;
    $url = "$server/mc_rpc_handler.php";
    $req->authenticator = $mc_config->authenticator;
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

function mc_create_observation($julian_date, $polarization, $length_days) {
    $req = new StdClass;
    $req->operation = 'create_observation';
    $req->julian_date = $julian_date;
    $req->polarization = $polarization;
    $req->length_days = $length_days;
    return mc_do_http_post($req);
}

?>
