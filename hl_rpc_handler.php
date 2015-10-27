<?php

// CGI handler for Librarian RPCs

error_reporting(E_ALL);
ini_set('display_errors', true);
ini_set('display_startup_errors', true);

require_once("hl_db.inc");

init_db(LIBRARIAN_DB_NAME);

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
    if (!observation_insert($req)) {
        error(db_error());
        return;
    }
    $reply = success();
    $reply->id = insert_id();
    echo json_encode($reply);
}

// handler for create file RPC
//
function create_file($req) {
    $source = source_lookup_auth($req->authenticator);
    if (!$source) {
        error("auth failure");
        return;
    }
    $req->create_time = time();
    $req->source_id = $source->id;
    $store = store_lookup_name($req->store_name);
    if (!$store) {
        error("bad store name");
        return;
    }
    $req->store_id = $store->id;

    if (!$req->md5) {
        ($req->size, $req->md5) = get_file_info($store, $req->file_name);
        if (!$req->md5) {
            error("couldn't get MD5");
            return;
        }
    }
    if (!file_insert($req)) {
        error(db_error());
        return;
    }
    store_update($store->id, "used = used+$req->size");
    $reply = success();
    $reply->id = insert_id();
    echo json_encode($reply);
}

function delete_file($req) {
    $source = source_lookup_auth($req->authenticator);
    if (!$source) {
        error("auth failure");
        return;
    }
    $store = store_lookup_name($req->store_name);
    if (!$store) {
        error("bad store name");
        return;
    }
    $file = file_lookup_name_store($req->name, $store->id);
    if (!$file) {
        error("no such file");
        return;
    }
    $now = time();
    $ret = file_update($file->id, "deleted=1, deleted_time=$now");
    if (!$ret) {
        error(db_error());
        return;
    }
    store_update($store->id, "used = used-$file->size");
    echo json_encode(success());
}

function create_task($req) {
    $source = source_lookup_auth($req->authenticator);
    if (!$source) {
        error("auth failure");
        return;
    }

    // do as much error checking here as we can
    //
    $store = store_lookup_name($req->local_store_name);
    if (!$store) {
        error("no such local store $req->local_store_name");
        return;
    }
    $file = file_lookup_name_store($req->file_name, $store->id);
    if (!$store) {
        error("no such file $req->file_name");
        return;
    }

    if (!task_insert($req)) {
        error(db_error());
        return;
    }
    $reply = success();
    $reply->id = insert_id();
    echo json_encode($reply);
}

function get_store_list($req) {
    $stores = store_enum('unavailable=0');
    $reply = success();
    $reply->stores = $stores;
    echo json_encode($reply);
}

function recommended_store($req) {
    $source = source_lookup_auth($req->authenticator);
    if (!$source) {
        error("auth failure");
        return;
    }
    $stores = store_enum('unavailable=0');
    foreach ($stores as $store) {
        $space = $store->capacity - $store->used;
        if ($file_size < $space) {
            $reply = success();
            $reply->store = $store;
            echo json_encode($reply);
            return;
        }
    }
    error("no store has sufficient free space");
    return;
}

$req = json_decode($_POST['request']);
switch ($req->operation) {
case 'create_observation': create_observation($req); break;
case 'create_file': create_file($req); break;
case 'delete_file': delete_file($req); break;
case 'create_task': create_task($req); break;
case 'get_store_list': get_store_list($req); break;
case 'recommended_store': recommended_store($req); break;
default: error("unknown op $req->operation");
}

?>
