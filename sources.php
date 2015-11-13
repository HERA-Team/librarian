<?php

require_once("hera_util.inc");
require_once("hl_db.inc");

init_db(get_server_config());

$sources = source_enum();
foreach ($sources as $source) {
    print_r($source);
}

?>
