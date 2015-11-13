<?php

// PHP binding of Librarian RPC interface
//
// The functions return a $reply object:
// $reply->success: 1 if success, 0 if failure
// $reply->message: if failure, error message
// $reply->id: DB of created record if any

require_once("hera_util.inc");

$hl_config = get_client_config();

function ret_struct($success, $message) {
    $ret = new StdClass;
    $ret->success = $success;
    $ret->message = $message;
    return $ret;
}

function get_site($site_name) {
    global $hl_config;
    if (!array_key_exists($site_name, $hl_config->sites)) {
        return null;
    }
    return $hl_config->sites->$site_name;
}

function hl_do_http_post($req, $site) {
    $url = "$site->url/hl_rpc_handler.php";
    $req->authenticator = $site->authenticator;
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
    return ret_struct(false, "can't parse JSON reply: $reply_json");
}

function create_observation(
    $site_name, $obs_id, $julian_date, $polarization, $length
) {
    $site = get_site($site_name);
    if (!$site) return ret_struct(false, "No such site $site_name");
    $req = new StdClass;
    $req->operation = 'create_observation';
    $req->id = $obs_id;
    $req->julian_date = $julian_date;
    $req->polarization = $polarization;
    $req->length = $length;
    return hl_do_http_post($req, $site);
}

function create_file(
    $site_name, $store_name, $file_name, $type, $obs_id, $size, $md5
) {
    $site = get_site($site_name);
    if (!$site) return ret_struct(false, "No such site $site_name");
    $req = new StdClass;
    $req->operation = 'create_file';
    $req->store_name = $store_name;
    $req->file_name = $file_name;
    $req->type = $type;
    $req->obs_id = $obs_id;
    $req->size = $size;
    $req->md5 = $md5;
    return hl_do_http_post($req, $site);
}

function delete_file(
    $site_name, $file_name, $store_name
) {
    $site = get_site($site_name);
    if (!$site) return ret_struct(false, "No such site $site_name");
    $req = new StdClass;
    $req->operation = 'delete_file';
    $req->name = $file_name;
    $req->store_name = $store_name;
    return hl_do_http_post($req, $site);
}

// get list of stores at a site
//
function get_store_list($site_name) {
    $site = get_site($site_name);
    if (!$site) return ret_struct(false, "No such site $site_name");
    $req = new StdClass;
    $req->operation = 'get_store_list';
    return hl_do_http_post($req, $site);
}

// wrapper around the above to look up a particular store
//
function lookup_store($site_name, $store_name) {
    $ret = get_store_list($site_name);
    if (!$ret->success) return $ret;
    foreach ($ret->stores as $store) {
        if ($store_name == $store->name) {
            $ret->store = $store;
            return $ret;
        }
    }
    $ret->success = false;
    $ret->message = "no such store";
    return $ret;
}

// get the recommended store for a file of given size
//
function recommended_store($site_name, $file_size) {
    $site = get_site($site_name);
    if (!$site) return ret_struct(false, "No such site $site_name");
    $req = new StdClass;
    $req->file_size = $file_size;
    $req->operation = 'recommended_store';
    return hl_do_http_post($req, $site);
}

function create_copy_task(
    $task_type,
    $local_site_name, $local_store_name, $file_name,
    $remote_site_name, $remote_store_name,
    $delete_when_done
) {
    $site = get_site($local_site_name);
    if (!$site) return ret_struct(false, "No such site $local_site_name");
    $req = new StdClass;
    $req->operation = 'create_copy_task';
    $req->task_type = $task_type;
    $req->local_store_name = $local_store_name;
    $req->file_name = $file_name;
    $req->remote_site_name = $remote_site_name;
    $req->remote_store_name = $remote_store_name;
    $req->delete_when_done = $delete_when_done?1:0;
    return hl_do_http_post($req, $site);
}

?>
