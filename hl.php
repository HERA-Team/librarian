<?php

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

function file_search_form() {
    page_head("Search files");
    echo '
        <form role="form" action="hl.php">
    ';
    show_source_select();
    echo '
        <button type="submit" name="action" value="search" class="btn btn-default">Submit</button>
        </form>
    ';
    page_tail();
}

function file_search_action() {
    page_head("Files");
    table_start();
    table_header(array("Name", "Created", "Source", "Size", "Store"));
    $clause = '';
    $source_id = get_int('source_id');
    if ($source_id) {
        $clause = "file.source_id = $source_id";
    }
    $files = file_enum($clause);
    foreach ($files as $file) {
        $source = source_lookup_id($file->source_id);
        $store = store_lookup_id($file->store_id);
        table_row(array(
            $file->name,
            time_str($file->create_time),
            $source->name,
            size_str($file->size),
            $store->name
        ));
    }
    table_end();
    page_tail();
}

function show_stores() {
    page_head("Storage");
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

if (!init_db(LIBRARIAN_DB_NAME)) {
    error_page("can't open DB");
}

$action = get_str("action", true);
switch ($action) {
case 'search':
    file_search_action(); break;
case 'stores':
    show_stores(); break;
default:
    file_search_form(); break;
}

?>
