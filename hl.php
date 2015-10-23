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
    $source_id = get_int('source_id');
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
    $source_id = get_int('source_id', true);
    if ($source_id) {
        $clause .= " and file.source_id = $source_id";
        $title = "Files from source $source_id";
    }
    $obs_id = get_int('obs_id', true);
    if ($obs_id) {
        $clause .= " and observation_id=$obs_id";
        $title = "Files from observation $obs_id";
    }
    page_head($title);
    $files = file_enum($clause);
    foreach ($files as $file) {
        $source = source_lookup_id($file->source_id);
        $store = store_lookup_id($file->store_id);
        table_row(array(
            "<a href=$file->url>$file->name</a>",
            time_str($file->create_time),
            $file->observation_id,
            $source->name,
            size_str($file->size),
            $store->name,
            $file->path
        ));
    }
    table_end();
    page_tail();
}

function show_stores() {
    page_head("Stores");
    table_start();
    table_header(array("Name", "Capacity", "Used", "% used"));
    $stores = store_enum();
    foreach ($stores as $store) {
        table_row(array(
            $store->name,
            size_str($store->capacity),
            size_str($store->used),
            progress_bar(100*$store->used/$store->capacity)
        ));
    }
    table_end();
    page_tail();
}

function show_tasks() {
    page_head("Tasks");
    table_start();
    table_header(array("Created", "File", "Local", "Remote", "Last error"));
    $tasks = task_enum();
    foreach ($tasks as $task) {
        table_row(array(
            time_str($task->create_time),
            $task->file_name,
            $task->local_store,
            $task->remote_site.': '.$task->remote_store,
            $task->last_error
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
default:
    obs_search_form(); break;
}

?>
