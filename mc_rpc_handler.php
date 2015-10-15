<?php

// handler for HERA M&C RPCs

error_reporting(E_ALL);
ini_set('display_errors', true);
ini_set('display_startup_errors', true);

require_once("mc_db.inc");

init_db(MC_DB_NAME);

// return JSON error reply
//
function error($msg) {
    $reply = new StdClass;
    $reply->success = false;
    $reply->message = $msg;
    echo json_encode($reply);
}

// return success reply object
//
function success() {
    $reply = new StdClass;
    $reply->success = true;
    return $reply;
}

// handler for create observation RPC
//
function create_observation($req) {
    $source = source_lookup_auth($req->authenticator);
    if (!$source) {
        error("auth failure");
        return;
    }
    $req->source_id = $source->id;
    if (!observation_insert_mc($req)) {
        error(db_error());
        return;
    }
    $reply = success();
    $reply->id = insert_id();
    echo json_encode($reply);
}

function create_status($req) {
    $source = source_lookup_auth($req->authenticator);
    if (!$source) {
        error("auth failure");
        return;
    }
    $req->source_id = $source->id;
    if (!status_insert($req)) {
        error(db_error());
        return;
    }
    $reply = success();
    $reply->id = insert_id();
    echo json_encode($reply);
}

$req = json_decode($_POST['request']);
switch ($req->operation) {
case 'create_observation':
    create_observation($req);
    break;
case 'create_status':
    create_status($req);
    break;
default:
    error("unknown op $req->operation");
}

?>
