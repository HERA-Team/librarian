Overview of the HERA Librarian
==============================

The Librarian is the data management system for the HERA project. At its core,
the Librarian’s sole job is to keep track of data products and help you to
retrieve them later.

In practice, the Librarian is a group of web applications and databases,
running both at the Karoo site and at HERA member institutions. The intention
is that most of your interactions with the Librarian will be through its
various web user interfaces. However, the Librarian also offers APIs that
other programs can invoke. This is most easily done in Python but there’s no
reason you couldn’t write such programs in other languages.


Table of Contents
-----------------

* [Foundational concepts](#foundational-concepts)
* [Data storage and distribution: the big picture](#data-storage-and-distribution-the-big-picture)
* [Programmatic access to the Librarian](#programmatic-access-to-the-librarian)
* [Practicalities: setting up the Librarian client](#practicalities-setting-up-the-librarian-client)
* [Practicalities: ingesting new files](#practicalities-ingesting-new-files)


Foundational concepts
---------------------

The Librarian keeps track of files. Each file has a unique name that may *not*
include directory components: `20200317_calibration_params.hdf` is acceptable,
but `20200317/calibration_params.hdf` is not.

Each file is immutable. Once you upload a file of a given name, you cannot
upload a new, different version. This is a powerful and important invariant to
maintain for reproducibility and synchronization. If you have data products
that you expect will need updating, devise a naming scheme for them that
allows you to distinguish different versions, and then maintain a list of
"most recent versions" outside of the Librarian (e.g., in a text file in a Git
repository, so that its history can be tracked and it can easily be processed
programmatically).

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
named `20200317_calibration_params.hdf` without actually having access to a
copy of it. In the other direction, it is possible for the Librarian’s data
storage to contain multiple instances of the same file — this may not be as
silly as it sounds of one of its storage hosts is flaky, or different storage
hosts are connected to different portions of the local network. When
processing data, the first step is often to ask the Librarian for the specific
paths where instances of your files of interest may be found.


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
as usual.

Finally, you need to set up the client configuration file that tells the code
how to contact a Librarian server. Because there are multiple servers, the
client has the idea of different Librarian “connections” that you can choose
from. Each connection has a name and specifies a server to contact and a
password to use.

The configuration file is named `~/.hl_client.cfg`. It is in
[JSON](http://www.json.org/) format.



Practicalities: Setting up the Librarian Client
-----------------------------------------------

The Librarian “client” is the Python module that lets you run or use programs
that talk to a Librarian server. Our goal is that most times you need to
interact with the Librarian, you’ll be able to do everything you want without
having to install anything. But sometimes you’re going to need the client. To
install it:

1. Are you working on the Karoo? The standard `obs` user already has the
   client code set up. You don’t need to do anything.
2. Install the `hera_librarian` Python module contained in this repository.
   
3. Set up the client configuration file as described below.




Practicalities: Ingesting New Files
-----------------------------------

Say you have created a shiny new data file that deserves to be enshrined in
the Librarian. How do you make that happen?

You should use the [upload_to_librarian.py](../scripts/upload-to-librarian.py)
program. This tool uses the Librarian “client” modules, so you need to set
those up first as described in
[Programmatic Access to the Librarian](#programmatic-access-to-the-librarian).
