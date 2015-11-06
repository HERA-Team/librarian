#! /usr/bin/env php
<?php
// copy_maker.php args
// start copy tasks for files that
// - currently have no copy task
// - satisfy the criteria given by args
//
// copy to the given remote site and store
//
// args:
// --filename_like expr
//   filename matches MySQL "like" expr (% is wildcard)
// --type t
//   file type is t
// --remote-site site-name
// --remote-store store-name

require_once("hl_db.inc");
require_once("hl_rpc_client.php");

function copy_maker($query) {
    $files = enum_general($query);
    $n = 0;
    foreach ($files as $file) {
        $local_store = store_lookup_id($file->store_id);
        $ret = create_task(TASK_TYPE_PUSH, $local_store_name, $file->name,
            $remote_site, $remote_store, false
        );
        if (!$ret->success) {
            echo "error: $ret->message\n";
            continue;
        }
        $n++;
    }
    echo "started $n copies\n";
}

init_db(LIBRARIAN_DB_NAME);

$filename_pattern = null;
$type = null;
$remote_site = null;
$remote_store = null;

for ($i=1; $i<$argc; $i++) {
    if ($argv[$i] == "--filename_like") {
        $filename_pattern = $argv[++$i];
    } else if ($argv[$i] == "--type") {
        $type = $argv[++$i];
    } else if ($argv[$i] == "--remote_site") {
        $remote_site = $argv[++$i];
    } else if ($argv[$i] == "--remote_store") {
        $remote_store = $argv[++$i];
    } else {
        die ("bad arg ".$argv[$i]."\n");
    }
}

if (!$remote_site || !$remote_store) {
    die ("Usage\n");
}

$query = "select * from file left join task on file.name = task.file_name where task.id is null";

if ($filename_pattern) {
    $filename_pattern = $link->escape_string($filename_pattern);
    $query .= " and file.name like '$filename_pattern'";
}
if ($type) {
    $type = $link->escape_string($type);
    $query .= " and type='$type'";
}

//echo $query;
copy_maker($query);

?>
