<?php

// CGI handler for Librarian RPCs

error_reporting(E_ALL);
ini_set('display_errors', true);
ini_set('display_startup_errors', true);

require_once("hl_db.inc");
require_once("hera_util.inc");

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

// get size and md5 of file or dir
//
function get_file_info($store, $file_name) {
    $path = "$store->path_prefix/$file_name";
    if ($store->ssh_prefix) {
        // if file is remote, issue ssh commands to storage host
        //
        $remote_cmd = "bash -c \"if [ -d $path ]; then zip $path | md5sum; else md5sum $path; fi\"";
        $cmd = "ssh $store->ssh_prefix '$remote_cmd'";
        $out = exec($cmd);
        $x = preg_split('/\s+/', $out);
        $md5 = $x[0];

        $remote_cmd = "bash -c \"if [ -d $path ]; then du -b $path; else wc -c $path; fi\"";
        $cmd = "ssh $store->ssh_prefix '$remote_cmd'";
        $out = exec($cmd);
        $x = preg_split('/\s+/', $out);
        $size = $x[0];
    } else {
        // File is local
        //
        if (is_dir($path)) {
            $out = exec("zip $path | md5sum");
            $x = preg_split('/\s+/', $out);
            $md5 = $x[0];

            $out = exec("du -b $path");
            $x = preg_split('/\s+/', $out);
            $size = $x[0];
        } else {
            $md5 = md5_file($path);
            $size = filesize($path);
        }
    }
    if (!is_numeric($size) || strlen($md5) != 32) {
        return array(-1, '');
    }
    return array($size, $md5);
}

// handler for create observation RPC
//
function create_observation($req) {
    if (!observation_insert($req)) {
        error(db_error());
        return;
    }
    $reply = success();
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
        list($size, $md5) = get_file_info($store, $req->file_name);
        if ($size < 0 || !$md5) {
            error("couldn't get file info");
            return;
        }
        $req->size = $size;
        $req->md5 = $md5;
    }

    $result = file_insert($req);
    if (!$result) {
        error(db_error());
        return;
    }
    list($id) = pg_fetch_row($result);
    //store_update($store->id, "used = used+$req->size");
    $reply = success();
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
    $ret = file_update($file->id, "deleted=1, extract(epoch from deleted_time)=$now");
    if (!$ret) {
        error(db_error());
        return;
    }
    store_update($store->id, "used = used-$file->size");
    echo json_encode(success());
}


function create_history($req) {
    // This RPC call creates a new record in the "history" table. It takes the
    // following arguments:
    //
    // authenticator -- (weak) proof that we're a valid requestor
    // store_name    -- the name of the store on which the associated file resides
    // file_name     -- the name of the associated file within its store
    // type          -- the type of this history event
    // payload       -- the data associated with this event
    //
    // "type" and "payload" do not currently have any enforced structure,
    // although "type" should be written in a hierarchical dot-separated form
    // such as "rtp.processed". The interpretation of "payload" depends on
    // "type"; we suggest JSON encoding for flexibility and future-proofness.

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

    $file = file_lookup_name_store($req->file_name, $store->id);
    if (!$file) {
        error("no such file");
        return;
    }

    if (empty ($req->type)) {
        error("missing create_history event type");
        return;
    }

    if (empty ($req->payload)) {
        error("missing create_history event payload");
        return;
    }

    $req->file_id = $file->id;
    $ret = history_insert($req);
    if (!$ret) {
        error(db_error());
        return;
    }

    echo json_encode(success());
}


function list_files_without_history_item($req) {
    // Return a list of files that do not have a particular kind of history
    // item. The RTP system uses this feature to identify new files that the
    // correlator has delivered to the Librarian that it needs to process.
    // Arguments are:
    //
    // authenticator -- (weak) proof that we're a valid requestor
    // source        -- the name of the source of the files we want
    // hist_type     -- the type of the history item we use to exclude files
    //
    // Right now we hardcode a limit of 1000 results being returned.
    //
    // TODO: document the fields that we return in the 'files' JSON element.

    $authsource = source_lookup_auth($req->authenticator);
    if (!$authsource) {
        error("auth failure");
        return;
    }

    $source = source_lookup_name($req->source);

    if (empty ($req->hist_type)) {
        error("missing hist_type in list_files_without_history_item");
        return;
    }

    $hist_type = db_escape ($req->hist_type);

    $files = enum_general(<<<SQL
SELECT *, extract(epoch from create_time) as create_time, extract(epoch from deleted_time) as deleted_time
FROM file
WHERE source_id = $source->id
AND deleted = 0
AND id NOT IN (
  SELECT file_id
  FROM history
  WHERE type = '$hist_type'
)
LIMIT 1000
SQL
        );

    // Include some information that's needed for RTP ingest. This is not
    // awesome SQL-wise but ... meh. It's not like the store table is going to
    // be changing out from under us (famous last words?).

    $store_map = array();
    $stores = enum ('store');

    foreach ($stores as $store) {
        $store_map[$store->id] = $store;
    }

    // Clean up returned info; make sure not to leak database internals.

    foreach ($files as $file) {
        $file->store_ssh_prefix = $store_map[$file->store_id]->ssh_prefix;
        $file->store_path_prefix = $store_map[$file->store_id]->path_prefix;

        unset($file->id);
        unset($file->store_id);
        unset($file->source_id);
    }

    $reply = success();
    $reply->files = $files;
    echo json_encode($reply);
}

function create_copy_task($req) {
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

    $req->local_store_id = $store->id;
    if (!task_insert($req)) {
        error(db_error());
        return;
    }
    $reply = success();
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
        if ($req->file_size < $space) {
            $reply = success();
            $reply->store = $store;
            echo json_encode($reply);
            return;
        }
    }
    error("no store has sufficient free space");
    return;
}

$config = get_server_config();
if (!$config) {
    error("no config file");
}
if (!init_db($config)) {
    error("can't connect to DB");
}

$req = json_decode($_POST['request']);
switch ($req->operation) {
case 'create_observation': create_observation($req); break;
case 'create_file': create_file($req); break;
case 'list_files_without_history_item': list_files_without_history_item($req); break;
case 'delete_file': delete_file($req); break;
case 'create_history': create_history($req); break;
case 'create_copy_task': create_copy_task($req); break;
case 'get_store_list': get_store_list($req); break;
case 'recommended_store': recommended_store($req); break;
default: error("unknown op $req->operation");
}

?>
