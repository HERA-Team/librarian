<?php

require_once("hl_web_util.inc");
require_once("hl_db.inc");

function show_files() {
    page_head("Files");
    table_start();
    table_header(array("name", "creator"));
    $files = file_enum('');
    foreach ($files as $file) {
        table_row(array($file->name, $file->size));
    }
    table_end();
    page_tail();
}

if (!init_db()) {
    error_page("can't open DB");
}

show_files();

?>
