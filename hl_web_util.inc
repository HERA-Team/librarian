<?php

function sidebar($cols, $items) {
    echo "
        <div class=\"col-*-$cols sidebar\">
            <ul class=\"nav nav-sidebar\">
    ";
    foreach ($items as $item) {
        echo "<li><a href=\"".$item[0]."\">".$item[1]."</a></li>\n";
    }
    echo "
            </ul>
        </div>
    ";
}

function navbar($title, $items_left, $items_right) {
    echo '
<nav class="navbar navbar-inverse">
  <div class="container-fluid">
    <div class="navbar-header">
      <a class="navbar-brand" href="hl.php">'.$title.'</a>
    </div>
    <div>
      <ul class="nav navbar-nav">
';
    foreach ($items_left as $item) {
        echo '<li><a href="'.$item[1].'">'.$item[0].'</a></li>
        ';
    }
    echo '
      </ul>
    ';
    if (count($items_right)) {
        echo '<ul class="nav navbar-nav navbar-right">
        ';
        foreach ($items_right as $item) {
            echo '
                <li><a href="'.$item[1].'">'.$item[0].'</a></li>
            ';
        }
        echo '</ul>
        ';
    }
    echo '
    </div>
  </div>
</nav>
';
}

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
';
    navbar("HERA Librarian",
        array(
            array('Files', 'hl.php'),
            array('Storage', 'hl.php?action=stores'),
        ),
        array(array('Contact', 'hl.php'))
    );
echo '
    <div class="container-fluid">
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

function get_str($name, $optional=false) {
    if (isset($_GET[$name])) {
        $x = $_GET[$name];
    } else {
        if (!$optional) {
            error_page("missing or bad parameter: $name");
        }
        $x = null;
    }
    return $x;
}

function get_int($name, $optional=false) {
    $x=null;
    if (isset($_GET[$name])) $x = $_GET[$name];
    if (!is_numeric($x)) {
        if ($optional) {
            if ($x) {
                error_page("$name not numeric");
            }
            return null;
        } else {
            error_page("$name missing");
        }
    }
    return (int)$x;
}

function time_str($x) {
    if ($x == 0) return "---";
    return gmdate('j M Y, G:i:s', $x) . " UTC";
}

function size_str($x) {
    $kilo = 1024.;
    $mega = $kilo*1024.;
    $giga = $mega*1024.;
    $tera = $giga*1024.;

    if ($x >= $tera) {
        $y = number_format($x/$tera, 2);
        return "$y TB";
    }
    if ($x >= $giga) {
        $y = number_format($x/$giga, 2);
        return "$y GB";
    }
    if ($x >= $mega) {
        $y = number_format($x/$mega, 2);
        return "$y MB";
    }
    if ($x >= $kilo) {
        $y = number_format($x/$kilo, 2);
        return "$y KB";
    }
    return "$x bytes";
}

function progress_bar($pct) {
    $p = (int)$pct;
    return '
        <div class="progress">
            <div class="progress-bar" role="progressbar" aria-valuenow="'.$pct.'"
                aria-valuemin="0" aria-valuemax="100" style="width:'.$pct.'%">
                '.$p.'%
            </div>
        </div>
    ';
}

?>
