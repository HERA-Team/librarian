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

function show_site_select() {
    echo '<div class="form-group">
        <label for="site_id">Site:</label>
        <select name=site_id>
        <option value=0> All
    ';
    $sites = site_enum();
    foreach ($sites as $s) {
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
    table_header(array("name", "source", "size"));
    $clause = '';
    $source_id = get_int('source_id');
    if ($source_id) {
        $clause = "source_id = $source_id";
    }
    $files = file_enum($clause);
    foreach ($files as $file) {
        $source = source_lookup_id($file->source_id);
        table_row(array($file->name, $source->name, $file->size));
    }
    table_end();
    page_tail();
}

if (!init_db()) {
    error_page("can't open DB");
}

$action = get_str("action", true);
if ($action == 'search') {
    file_search_action();
} else {
    file_search_form();
}

?>
