# Overview of the HERA Librarian

The Librarian is the HERA archive system. Its job is to track files deemed
essential to the HERA project, to manage where they are stored, move them
between archive sites, and to provide a searchable interface for human and
computer users.

It is implemented as a database-backed web server that interacts with online
subsystems via an API and humans via a web interface.


### Table of Contents

* [Foundational concepts](#foundational-concepts)
* [Data storage and distribution: the big picture](#data-storage-and-distribution-the-big-picture)


## Foundational concepts

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


## Data Storage and Distribution: The Big Picture

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
