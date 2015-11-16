<?php

// Librarian web interface.

require_once("hera_util.inc");
require_once("hl_web_util.inc");
require_once("hl_db.inc");

function show_source_select() {
    echo '<div class="form-group">
        <label for="source_id">Source:</label>
        <select name=source_id>
        <option value=0> All
    ';
    $sources = source_enum();
    foreach ($sources as $source) {
        echo "<option value=$source->id> $source->name\n";
    }
    echo '</select>
        </div>
    ';
}

function show_store_select() {
    echo '<div class="form-group">
        <label for="store_id">Store:</label>
        <select name=store_id>
        <option value=0> All
    ';
    $stores = store_enum();
    foreach ($stores as $s) {
        echo "<option value=$s->id> $s->name\n";
    }
    echo '</div>
    ';
}

function obs_search_form() {
    page_head("Search observations");
    echo '<form role="form" action="hl.php">
    ';
    show_source_select();
    echo '
        <button type="submit" name="action" value="obs_search_action" class="btn btn-default">Submit</button>
        </form>
    ';
    page_tail();
}

function obs_search_action() {
    page_head("Observations");
    table_start();
    table_header(array("ID (click for files)", "Date", "Source", "Polarization", "Length (seconds)"));
    $clause = '';
    $source_id = get_num('source_id', true);
    if ($source_id) {
        $clause = "source_id = $source_id";
    }
    $obs = observation_enum($clause);
    foreach ($obs as $ob) {
        $source = source_lookup_id($ob->source_id);
        table_row(array(
            "<a href=hl.php?action=file_search_action&obs_id=$ob->id>$ob->id</a>",
            time_str($ob->julian_date),
            $source->name,
            $ob->polarization,
            $ob->length
        ));
    }
    table_end();
    page_tail();
}

function file_search_form() {
    page_head("Search files");
    echo '<form role="form" action="hl.php">
    ';
    show_source_select();
    echo '
        <button type="submit" name="action" value="file_search_action" class="btn btn-default">Submit</button>
        </form>
    ';
    page_tail();
}

function file_search_action() {
    table_start();
    table_header(array("Name<br><span class=small>click to download</span>", "Created", "Observation", "Type", "Source", "Size", "Store"));
    $clause = 'true';
    $source_id = get_num('source_id', true);
    $title = "All files";
    if ($source_id) {
        $clause .= " and file.source_id = $source_id";
        $title = "Files from source $source_id";
    }
    $obs_id = get_num('obs_id', true);
    if ($obs_id) {
        $clause .= " and obs_id=$obs_id";
        $title = "Files from observation $obs_id";
    }
    $store_id = get_num('store_id', true);
    if ($store_id) {
        $store = store_lookup_id($store_id);
        $clause .= " and store_id=$store_id";
        $title = "Files from store $store->name";
    }
    page_head($title);
    $files = file_enum($clause);
    foreach ($files as $file) {
        $source = source_lookup_id($file->source_id);
        $store = store_lookup_id($file->store_id);
        table_row(array(
            "<a href=$store->http_prefix/$file->name>$file->name</a>",
            time_str($file->create_time),
            "<a href=hl.php?obs_id=$file->obs_id&action=file_search_action>$file->obs_id</a>",
            $file->type,
            $source->name,
            size_str($file->size),
            "<a href=hl.php?action=store&id=$store->id>$store->name</a>"
        ));
    }
    table_end();
    page_tail();
}

function show_stores() {
    page_head("Stores");
    table_start();
    table_header(array(
        "Name<br><small>Click for details</small>",
        "Files<br><small>Click to view</small>",
        "Capacity", "Used", "% used", "Available"
    ));
    $stores = store_enum();
    foreach ($stores as $store) {
        $nfiles = file_count("store_id=$store->id");
        table_row(array(
            "<a href=hl.php?action=store&id=$store->id>$store->name</a>",
            "<a href=hl.php?action=file_search_action&store_id=$store->id>$nfiles</a>",
            size_str($store->capacity),
            size_str($store->used),
            progress_bar(100*$store->used/$store->capacity),
            $store->unavailable?"No":"Yes"
        ));
    }
    table_end();
    echo '
        <a href="hl.php?action=edit_store_form">
        <button type="button" class="btn btn-default">
        Add store
        </button>
        </a>
    ';
    page_tail();
}

function edit_store_form() {
    $id = get_num("id", true);
    if ($id) {
        $store = store_lookup_id($id);
        if (!$store) {
            error_page("no such store");
        }
        page_head("Edit $store->name");
    } else {
        $store = new StdClass;
        $store->name = '';
        $store->capacity = 0;
        $store->used = 0;
        $store->rsync_prefix = '';
        $store->http_prefix = '';
        $store->path_prefix = '';
        $store->ssh_prefix = '';
        $store->unavailable = false;
        page_head("Add store");
    }
    echo '<form role="form" action="hl.php" method="get">
        <input type="hidden" name="action" value="edit_store_action">
    ';
    if ($id) {
        echo '<input type="hidden" name="id" value="'.$id.'">
        ';
    }
    form_item("Name:", "text", "name", $store->name);
    form_item("Capacity (GB):", "text", "capacity", $store->capacity/GIGA);
    form_item("Used (GB):", "text", "used", $store->used/GIGA);
    form_item("rsync prefix:", "text", "rsync_prefix", $store->rsync_prefix);
    form_item("HTTP prefix:", "text", "http_prefix", $store->http_prefix);
    form_item("Path prefix:", "text", "path_prefix", $store->path_prefix);
    form_item("SSH prefix:", "text", "ssh_prefix", $store->ssh_prefix);
    form_checkbox("Unavailable:", "unavailable", $store->unavailable);
    form_submit_button("Submit");
    echo '</form>
    ';
    page_tail();
}

function show_store() {
    $id = get_num("id");
    $store = store_lookup_id($id);
    if (!$store) {
        error_page("no such store");
    }
    page_head("$store->name");
    table_start();
    //row2("Name", $store->name);
    row2("Capacity (GB)", $store->capacity/GIGA);
    row2("Used (GB)", $store->used/GIGA);
    row2("rsync prefix", $store->rsync_prefix);
    row2("HTTP prefix", $store->http_prefix);
    row2("path prefix", $store->path_prefix);
    row2("SSH prefix", $store->ssh_prefix);
    row2("Available", $store->unavailable?"No":"Yes");
    row2("", button("Edit", "hl.php?action=edit_store_form&id=$store->id"));
    table_end();
    page_tail();
}

function edit_store_action() {
    $store = new StdClass;
    $store->name = get_str("name");
    $store->capacity = get_num("capacity")*GIGA;
    $store->used = get_num("used")*GIGA;
    $store->rsync_prefix = get_str("rsync_prefix");
    $store->http_prefix = get_str("http_prefix");
    $store->path_prefix = get_str("path_prefix");
    $store->ssh_prefix = get_str("ssh_prefix");
    $store->unavailable = get_str("unavailable", true)?true:false;
    $id = get_num("id");
    if ($id) {
        $ret = store_update_all($id, $store);
    } else {
        $store->create_time = time();
        $ret = store_insert($store);
    }
    if (!$ret) {
        error_page("database error");
    }
    redirect("hl.php?action=stores");
}

function task_phase_name($task) {
    switch ($task->state) {
    case 0: return "rsync";
    case 1: return "register";
    case 2: return "delete";
    }
    return "unknown";
}

function task_status($task) {
    if ($task->completed) {
        return "Completed ".time_str($task->completed_time);
    }
    if ($task->in_progress) {
        return "In progress: ".task_phase_name($task);
    }
    return "Waiting to start";
}

function show_tasks() {
    page_head("Tasks");
    table_start();
    table_header(array("ID", "Created", "File", "Local", "Remote", "Status", "Last error"));
    $tasks = task_enum();
    foreach ($tasks as $task) {
        $store = store_lookup_id($task->local_store_id);
        table_row(array(
            $task->id,
            time_str($task->create_time),
            $task->file_name,
            $store->name,
            $task->remote_site.': '.$task->remote_store,
            task_status($task),
            $task->last_error.' ('.time_str($task->last_error_time).')'
        ));
    }
}

if (!init_db(get_server_config())) {
    error_page("can't open DB");
}

$action = get_str("action", true);
switch ($action) {
case 'edit_store_action':
    edit_store_action(); break;
case 'edit_store_form':
    edit_store_form(); break;
case 'file_search_action':
    file_search_action(); break;
case 'file_search_form':
    file_search_form(); break;
case 'obs_search_action':
    obs_search_action(); break;
case 'store':
    show_store(); break;
case 'stores':
    show_stores(); break;
case 'tasks':
    show_tasks(); break;
default:
    if ($action) {
        error_page("Unknown action '$action'");
    }
    file_search_action(); break;
}

?>
