<?php

// utility functions for Librarian web interface

error_reporting(E_ALL);
ini_set('display_errors', true);
ini_set('display_startup_errors', true);

require_once("hera_util.inc");

// show sidebar with given list of items
//
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

// show horizontal navbar with given items
//
function navbar($title, $items_left, $items_right) {
    echo '
<nav class="navbar navbar-default">
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
    $config = get_server_config();
    $site_title = $config->title;
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
    navbar("HERA Librarian ($site_title)",
        array(
            array('Observations', 'hl.php?action=obs_search_action'),
            array('Files', 'hl.php?action=file_search_form'),
            array('Stores', 'hl.php?action=stores'),
            array('Tasks', 'hl.php?action=tasks'),
        ),
        array(array('Logout', 'hl.php?action=logout'))
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
    exit;
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

// use for name/value lists
//
function row2($x, $y) {
    echo "<tr><td width=\"40%\" class=\"text-right\"><strong>$x</strong></td><td>$y</td></tr>
    ";
}

function table_end() {
    echo '
        </tbody>
        </table>
        </div>
    ';
}

// return a GET string argument.
// If $optional is true and arg is missing, return null
//
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

// same, numeric argument
//
function get_num($name, $optional=false) {
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
    return 0+$x;
}

// convert Unix time to string
//
function time_str($x) {
    if ($x == 0) return "---";
    return gmdate('j M Y, G:i:s', $x) . " UTC";
}

define('KILO', 1024);
define('MEGA', 1024*KILO);
define('GIGA', 1024*MEGA);
define('TERA', 1024*GIGA);

// convert size (in bytes) to string
//
function size_str($x) {
    if ($x >= TERA) {
        $y = number_format($x/TERA, 2);
        return "$y TB";
    }
    if ($x >= GIGA) {
        $y = number_format($x/GIGA, 2);
        return "$y GB";
    }
    if ($x >= MEGA) {
        $y = number_format($x/MEGA, 2);
        return "$y MB";
    }
    if ($x >= KILO) {
        $y = number_format($x/KILO, 2);
        return "$y KB";
    }
    return "$x bytes";
}

// show progress bar
//
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

function form_item($label, $type, $name, $value) {
    echo '
        <div class="form-group">
        <label for="'.$name.'">'.$label.'</label>
        <input type="'.$type.'" class="form-control" name="'.$name.'" id="'.$name.'" value="'.$value.'">
        </div>
    ';
}

function form_checkbox($label, $name, $value) {
    $v = $value?"checked":"unchecked";
    echo '
        <div class="checkbox">
        <label><input type="checkbox" name="'.$name.'" '.$v.'>'.$label.'</label>
        </div>
    ';
}

function button($label, $url) {
    return "<a href=\"$url\" class=\"btn btn-default\" role=\"button\">$label</a>
    ";
}

function form_submit_button($label) {
    echo '<button type="submit" class="btn btn-default">'.$label.'</button>
    ';
}

function redirect($url) {
    Header("Location: $url");
}

?>
