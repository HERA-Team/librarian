<?php

function page_head($title) {
echo '
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <!-- The above 3 meta tags *must* come first in the head; any other head content must come *after* these tags -->
        <title>'.$title.'</title>

        <!-- Latest compiled and minified CSS -->
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css">

        <!-- Optional theme -->
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap-theme.min.css">

        <!-- HTML5 shim and Respond.js for IE8 support of HTML5 elements and media queries -->
        <!--[if lt IE 9]>
            <script src="https://oss.maxcdn.com/html5shiv/3.7.2/html5shiv.min.js"></script>
            <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
        <![endif]-->
    </head>
    <body>
    <div class="container">
';
echo "<h1>$title</h1>\n";
}

function page_tail() {
echo'
    </div>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js"></script>
    <!-- Include all compiled plugins (below), or include individual files as needed -->
    <!-- Latest compiled and minified JavaScript -->
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
    </body>
    </html>
';
}

function error_page($msg) {
    page_head("Error");
    echo $msg;
    page_tail();
}

function table_start() {
    echo '
        <div class="table-responsive">
        <table class="table table-striped">
    ';
}
function table_header($items) {
    echo '
        <thead>
        <tr>
    ';
    foreach ($items as $item) {
        echo "<th>$item</th>\n";
    }
    echo '
        </tr>
        </thead>
        <tbody>
    ';
}

function table_row($items) {
    echo "<tr>\n";
    foreach ($items as $item) {
        echo "<td>$item</td>\n";
    }
    echo "</tr>\n";
}

function table_end() {
    echo '
        </tbody>
        </table>
        </div>
    ';
}

?>
