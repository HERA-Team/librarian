<?php

// MySQL binding of RPC interface to create files / file instances
//
// The functions return a $reply object:
// $reply->success: 1 if success, 0 if failure
// $reply->message: if failure, error message

require_once("hl_util.inc");

function do_http_post($req) {
    $config = get_config();
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

function create_file($name, $size, $md5) {
    $req = new StdClass;
    $req->operation = 'create_file';
    $req->name = $name;
    $req->size = $size;
    $req->md5 = $md5;
    return do_http_post($req);
}

function create_file_instance($file_name, $site_name, $store_name) {
    $req = new StdClass;
    $req->operation = 'create_file_instance';
    $req->file_name = $file_name;
    $req->site_name = $site_name;
    $req->store_name = $store_name;
    return do_http_post($req);
}

?>
