# HERA Librarian: Staging to Lustre

“Staging” is the term we use for copying files out of the Librarian on to
HERA’s Lustre workspace at NRAO.

### Contents

- [Overview](#overview)
- [Staging through the web UI](#staging-through-the-web-ui)
- [Staging from the command line](#staging-from-the-command-line)
- [Staging from inside a Python program](#staging-from-inside-a-python-program)
- [Esoterica](#esoterica)


## Overview

In the Librarian, staging is intimately tied to [searching](Searching.md).
Normally, when you search for files, the result is that a list of the matching
files is shown to you in your web browser. But there is also a mode where the
“result” is to copy the matching files into a directory on Lustre.

For convenience, the NRAO Librarian web interface includes a shortcut to run a
staging search after you’ve run a standard search. If you scroll down to the
bottom of file search result page, there will be a section that allows you to
specify your NRAO username and a destination directory, then launch the
staging operation.

Staging can take some time, and our web interface isn’t fancy enough to be
able to report progress to you. To monitor progress, you should keep an eye on
some special files that the Librarian makes inside your destination directory.
One called `STAGING-IN-PROGRESS` will be there as long as staging is in
progress. After staging finishes, it will hopefully be replaced with one
called `STAGING-SUCCEEDED`.

If errors were encountered during staging, a file called `STAGING-ERRORS` will
appear instead. The contents of that file will report error messages that
occurred during the staging process, to the best of the Librarian’s ability to
figure out what happened.

Once staging is complete, that directory is yours to do whatever you want
with. Our Lustre space allocation is limited, so please delete the directory
when you’re done with it.


## Staging through the web UI

There are two main ways to stage files through the web UI. First, if you just
want to stage all of the files from a particular observation or observing
session, you can just click through to the detailed information page for the
observation or session in question. There will be a section that lets you type
in your Lustre destination directory and launch the operation.

For more complicated staging operations, you should first run a standard
search for files, selecting the “List of files” output format. Then, all you
have to do is:

1. Scroll down to the bottom of the results page to the “Staging to Lustre”
   section.
2. Enter in your NRAO username where prompted.
2. Optionally enter in the a sub-directory name in which to put these files.
3. Hit the “Launch” button.

Unless something went wrong the Librarian will tell you how many instances and
bytes it will copy to your destination, and it will get to work copying the
data.

Early tests indicate that the Librarian can copy to Lustre at a rate of 10–20
GB a minute.

The top of the file search result page includes a link entitled “Skip down to
the Lustre staging section”. This scrolls your browser window down to the
staging section, which is useful if your search matches a large number of
files.


## Staging from the command line

If your user account is set up with an installation of the Librarian client
Python module and you have set up your `~/.hl_client.cfg` file, you can also
launch a stage directly from the command line.

An example command is:

```
librarian_stage_files.py local /lustre/aoc/projects/hera/pwilliam/demo \
 '{"name-matches": "zen.2457644.%.xx.HH.uvc"}'
```

The three arguments are:

1. Which Librarian connection in your `~/.hl_client.cfg` file to use
2. The destination path on Lustre
3. The [JSON search specification](Searching.md) to use.

The JSON search string will typically include spaces and characters that are
special to the shell, so make sure to quote it appropriate. It usually
contains double-quotes, so it’s usually best to wrap it in single quotes for
the shell. Note that single-quoted shell strings do not have shell-variable
substitutions applied, though. When in doubt, just use the `echo` command to
see what the shell is doing to what you type.

The `librarian_stage_files.py` command also has an option called `--wait` or
`-w`. When specified, the program will not exit until the staging process is
done. This is useful for scripting.


## Staging from inside a Python program

The standard `hera_librarian` client module includes an API call that will
launch a staging operation. Once you’ve created a `LibrarianClient` client
object, the method is `launch_local_disk_stage_operation`. See the
implementation of the `librarian_stage_files.py` program for a usage example.

Right now, there is no built-in support for the client to wait for the staging
operation to complete. If you want that functionality, copy what
`librarian_stage_files.py` does.


## Esoterica

When staging through the web UI, the “launch” operation really does re-run
your search. This means that it is possible that the results will differ
slightly from the list of files shown to you, if files appeared or disappeared
in the time it took to run the search again.

It is important to keep in mind that the Librarian can only stage files that
it actually has copies of! A file might match a search, but if it has no
instances at NRAO, we can’t magically conjure one up. We don’t expect to delete
files from NRAO so this shouldn’t be an issue in practice.

The staging feature is fully generic, so it could be deployed on Librarians
installed elsewhere if desired. Under the hood, all its doing is copying files
from one place to another *on the host on which the Librarian server runs*. At
NRAO, both the storage disks and the Lustre look like local filesystems.
