<?php

// Librarian web interface.

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
    table_header(array("ID (click for files)", "Date", "Source", "Polarization", "Length (days)"));
    $clause = '';
    $source_id = get_num('source_id');
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
            $ob->length_days
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
    table_header(array("Name", "Created", "Observation", "Source", "Size", "Store", "Path"));
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
    page_head($title);
    $files = file_enum($clause);
    foreach ($files as $file) {
        $source = source_lookup_id($file->source_id);
        $store = store_lookup_id($file->store_id);
        table_row(array(
            "<a href=$store->http_prefix/$file->name>$file->name</a>",
            time_str($file->create_time),
            "<a href=hl.php?obs_id=$file->obs_id&action=file_search_action>$file->obs_id</a>",
            $source->name,
            size_str($file->size),
            $store->name,
            "$store->path/$file->name"
        ));
    }
    table_end();
    page_tail();
}

function show_stores() {
    page_head("Stores");
    table_start();
    table_header(array("Name<br><small>Click to edit</small>", "Capacity", "Used", "% used", "Available"));
    $stores = store_enum();
    foreach ($stores as $store) {
        table_row(array(
            "<a href=hl.php?action=edit_store_form&id=$store->id>$store->name</a>",
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
        page_head("Edit store $store->name");
    } else {
        $store = new StdClass;
        $store->name = '';
        $store->capacity = 0;
        $store->used = 0;
        $store->rsync_prefix = '';
        $store->http_prefix = '';
        $store->path = '';
        $store->ssh_host = '';
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
    form_item("Path:", "text", "path", $store->path);
    form_item("SSH host:", "text", "ssh_host", $store->ssh_host);
    form_checkbox("Unavailable:", "unavailable", $store->unavailable);
    form_submit_button("Submit");
    echo '</form>
    ';
}

function edit_store_action() {
    $store = new StdClass;
    $store->name = get_str("name");
    $store->capacity = get_num("capacity")*GIGA;
    $store->used = get_num("used")*GIGA;
    $store->rsync_prefix = get_str("rsync_prefix");
    $store->http_prefix = get_str("http_prefix");
    $store->path = get_str("path");
    $store->ssh_host = get_str("ssh_host");
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
    table_header(array("Created", "File", "Local", "Remote", "Status", "Last error"));
    $tasks = task_enum();
    foreach ($tasks as $task) {
        table_row(array(
            time_str($task->create_time),
            $task->file_name,
            $task->local_store,
            $task->remote_site.': '.$task->remote_store,
            task_status($task),
            $task->last_error.' ('.time_str($task->last_error_time).')'
        ));
    }
}

if (!init_db(LIBRARIAN_DB_NAME)) {
    error_page("can't open DB");
}

$action = get_str("action", true);
switch ($action) {
case 'file_search_action':
    file_search_action(); break;
case 'file_search_form':
    file_search_form(); break;
case 'obs_search_action':
    obs_search_action(); break;
case 'stores':
    show_stores(); break;
case 'tasks':
    show_tasks(); break;
case 'edit_store_form':
    edit_store_form(); break;
case 'edit_store_action':
    edit_store_action(); break;
default:
    file_search_action(); break;
}

?>
