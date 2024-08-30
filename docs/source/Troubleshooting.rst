Troubleshooting
===============

What do you do when things to bump in the night? The librarian is
an extremely simple application.

You Should be Backing Up
------------------------

Though the librarian itself is a tool for creating copies of data
that are distributed on different systems, you should not neglect
the librarian itself.

You should be regularly (ideally daily) backing up your librarian
database. If you are using postgres, this is very easily completed
by using `pg_dump` in, for example, a cron job. You could even
consider adding this backup as a new file in the librarian...


Recovering from Data Loss
-------------------------

Data loss is difficult to deal with. Below we look at a few cases
and how to recover from them.

Database Loss
^^^^^^^^^^^^^

Even with good backups, bad things can happen. You may lose the librarian
database. After recovering from a backup, you will have some missing rows.
This can be an unsolveable problem!

Why is this problem unsolvable in the librarian? Well, because we have
degeneracy between files and directories, there is no way of telling
from the contents of a store whether:

a) A single folder `hello/world` was ingested, or:
b) Two files `hello/world/1.txt` and `hello/world/2.txt`

were ingested. However, in most applications, you will have a standardised
file structure and a way to generate the list of files that were ingested.
If you have only ever ingested files, you can use the ``librarian-server-rebuild-database``
script. Its contents may be of help to you for writing your own rebuild
script appropriate for your own information. Do not use this script without
reading it!

One way to keep this information is in *other librarians*. You can gain
information about all of the files in the system by querying downstream
librarians.

Once you have a list of files and their metadata, you can create a
file and instance on your librarian using the ``add_file_row``
function in the librarian that lost the data.

But what about the other way around? You lost some information from
the downstream librarian? It is actually possible to recover the
information required by using the remote instances available
on the source librarian. You need to run the ``librarian-server-repair-database``
script twice, once at the source and then once at the destination.
This script even spot-checks files' checksums to make sure there
has been no data corruption.
