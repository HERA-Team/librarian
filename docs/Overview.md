Overview of the HERA Librarian
==============================

The Librarian is the HERA archive system. Its job is to track files deemed
essential to the HERA project, to manage where they are stored, move them
between archive sites, and to provide a searchable interface for human and
computer users.

It is implemented as a database-backed web server that interacts with online
subsystems via an API and humans via a web interface.


Table of Contents
-----------------

* [Foundational concepts](#foundational-concepts)
* [Data storage and distribution: the big picture](#data-storage-and-distribution-the-big-picture)
* [Logging into the Librarian](#logging-into-the-librarian)
* [Programmatic access to the Librarian](#programmatic-access-to-the-librarian)
* [Practicalities: ingesting new files](#practicalities-ingesting-new-files)


Foundational concepts
---------------------

The Librarian keeps track of files. Each file has a unique name that may *not*
include directory components: `cal.2557561.66007.phases.hdf` is acceptable,
but `2557561/cal.2457561.66007.phases.hdf` is not.

Each file is immutable. Once you upload a file of a given name, you cannot
update it — a new version must be given a new name. This is a powerful and
important invariant to maintain for reproducibility and synchronization. If
you have data products that you expect will need updating, devise a naming
scheme for them that allows you to distinguish different versions, and then
maintain a list of "most recent versions" outside of the Librarian (e.g., in a
text file in a Git repository, so that its history can be tracked and it can
easily be processed programmatically).

Each “file” is either a regular flat computer file, or a directory. The latter
feature can make life a bit of a hassle sometimes, but it is very valuable to
us to be able to upload MIRIAD datasets and store them as directories.

The Librarian is for tracking any kind of data product. We expect that its
contents will be dominated by visibility data, but any data file that we might
want to archive is fair game. This might include monitor-and-control
telemetry, OS log files, images, diagnostic output from large computational
jobs, etc.

There are actually multiple Librarians, running at different sites. They
operate independently but can talk to each other, so they form a loosely
coupled, distributed system.


Data Storage and Distribution: The Big Picture
----------------------------------------------

Each incarnation of the Librarian has:

* a database of known files and metadata about them,
* a web interface, and
* a storage backend where copies of files actually live.

One Librarian can send a copy of a file, and its associated metadata, to
another. This is how we implement data transfer from the Karoo the the US.

It’s important to understand that the Librarian distinguishes between the
abstract concept of a “file” and an actual copy of a given file, which it
calls a “file instance”. A Librarian may be aware of the existence of a file
named `cal.2557561.66007.phases.hdf` — and know some of its metadata — without
actually having a local copy of it. In the other direction, it is possible for
the Librarian’s data storage to contain multiple instances of the same file —
this may not be as silly as it sounds of one of its storage hosts is flaky, or
different storage hosts are connected to different portions of the local
network. When processing data, the first step is often to ask the Librarian
for the specific paths where instances of your files of interest may be found.


Logging into the Librarian
--------------------------

To access the Librarian web interface, you need to know the URL and an
“authenticator”, which is basically a password without a username. Some of
HERA’s Librarians are not visible on the open Internet, so you need to set up
an SSH tunnel in order to be able to connect to them.

The login info for HERA’s Librarians is not public. It may be found
[here on the HERA Wiki](http://herawiki.berkeley.edu/doku.php/librarian).


Programmatic Access to the Librarian
------------------------------------

Our goal is that most times you need to interact with the Librarian, you’ll be
able to do everything you want through the web user interface. But to automate
certain tasks, or do certain fancy things, you’ll need to install the
Librarian “client” Python module that can talk to Librarian servers using
their API. This section describes how to install the client and the important
concept of “connections” that the client uses.

To install the client, the first step is to ask yourself if you’re working on
the Karoo computers. If so, the shared `obs` account already has the client
set up, so you don’t need to do anything. Yay!

If you’re not using a pre-configured HERA software stack, the next step is
install the [hera_librarian](../hera_librarian) Python module contained in
this repository. All you have to do is run the [setup.py](../setup.py) script
as usual. The client code depends on Astropy and may need Aipy in certain rare
cases.

Finally, you need to set up the client configuration file that tells the code
how to contact a Librarian server. Because there are multiple servers, the
client has the idea of different Librarian “connections” that you can choose
from. Each connection has a name and specifies a server to contact and a
password to use.

The configuration file is named `~/.hl_client.cfg`. It is in
[JSON](http://www.json.org/) format. Its contents will depend on where your
account is located and which Librarians you need your programs to talk to. On
Folio, your file should look like:

```
{
    "connections": {
        "human@folio": {
            "url": "http://folio2:51110/",
            "authenticator": "HIDDEN-SECRET"
        }
    }
}
```

The “authenticator” field is a password so we can’t reproduce it here. As
mentioned above, you may find HERA’s connection information
[here on the HERA Wiki](http://herawiki.berkeley.edu/doku.php/librarian#authenticators_and_client_configuration_examples).


Practicalities: Ingesting New Files
-----------------------------------

Say you have created a shiny new data file that deserves to be enshrined in
the Librarian. How do you make that happen?

You should use the [upload_to_librarian.py](../scripts/upload-to-librarian.py)
program. This tool uses the Librarian “client” modules, so you need to set
those up first as described in
[Programmatic Access to the Librarian](#programmatic-access-to-the-librarian).

After installing the client modules, the
[upload_to_librarian.py](../scripts/upload-to-librarian.py) script should be
visible in your path. You can run it with the `--help` option to see its
self-contained help. The basic usage format is:

```
upload_to_librarian.py {connection} {local-path} {dest-path}
```

For instance:

```
upload_to_librarian.py human@folio cal.2557561.66007.phases.hdf 2557561/cal.2557561.66007.phases.hdf
```

*The name of your file is important for several reasons*. First, obviously,
the name is permanent and unchangeable, so once you upload a file you’re stuck
with it.

Second, remember how we said that each file is associated with an obsid? By
default, the uploader script will infer that obsid by looking for something in
the destination filename that looks like a fractional number — `2557561.66007`
in this case. It will treat it as a JD and then convert it to an obsid (which
is just the JD converted to a GPS time in integer seconds). So, the upshot is,
*make sure that your file name has a JD string in it that exactly matches the
JD string of a raw data set*. Also, make sure that no other parts of the
filename contain a sequence of `<digits>.<digits>` to avoid ambiguity. (It is
possible, but very inconvenient, to override this default inference behavior
using the not-really-documented `--meta=json-stdin` option.)

Second, each file has a “type” associated with it, which is a short string
describing its format. By default, the uploader script guesses it from the
final extension of the filename — the type would be “hdf” in the example
above. We don’t use these types right now, but please make sure the extension
is sensible.

Finally, you might recall that we said that Librarian filenames may not
contain directory parts — “/” characters. But in the example above, the
destination path has one! What gives? What’s happening here is that the
directory piece is a hint to the Librarian of what directory to stash the file
in *in its local storage*. The Official Librarian Name of the uploaded file
will always be the final piece of whatever path you give it, but this helps
keeps the files organized sensibly on the storage disks. By convention, files
are stored in directories named according to the integer part of their JD.
Also, note that the name of your file on disk does not need to match the name
that it will be given on the Librarian.
