Connecting Librarians
=====================

Instances of librarian servers can be connected together to form a network.
Unlike other rules-based systems, the librarian network is not a single inter-connected
whole with ingress and egress points, but rather each librarian is a monolith that
can push data to others. Each librarian can be disconnected or re-connected
to the 'network' at any time without data loss within its own domain.

It is crucial to understand that the abstracted 'file' concept within the
librarian only stores metadata about files. Real 'files' are stored on disk
and are tracked directly through linked 'instances' which are stored alongside
files. There is also a concept of a 'remote instance' which refers to a known
instance of a file existing on a different librarian. Each instance can also
be tagged as 'available' or 'unavailable'. If available, the file is available
at the ``instance_path`` that is stored in the instance metadata. If unavailable,
it has been deleted from disk, but the instances are still tracked.

Because of this decoupling
of metadata and data, there are several possible instance states of a file:

- A file may exist with just one instance on a single librarian. This is
  how every file starts, as it is `ingested <./Uploading.rst>`_.
- A file may exist with many instances on a single librarian. This is common
  with the SneakerNet transfer method, or when there are two classes of
  storage at a datacenter.
- A file may exist with many instances on many librarians. At the source
  librarian, the file will have its own local instances and remote instances
  corresponding *only to librarains directly connected to the source*. Instances
  that are 'two away' from the source are not tracked in the table.
- A file may exist with no local instances but known remote instances. This
  is the case when a file is deleted from an experiment site with limited storage
  but is kept on a central librarian.
- A file may exist with no instances at all. This is the case when a file is
  effectively deleted from the librarian.

The librarian network is hence a directed graph where each librarian is a node
and each connection is an edge. The librarian network is not a fully connected
graph, but rather a series of connected components. Each librarian can be
connected to any number of other librarians. This allows for a very flexible,
though admittedly fragile, system.

An example set of connections may be:

- Source Librarian is connected to Central Librarian
- Central Librarian is connected to Destination A
- Central Librarian is connected to Destination B

In this case, the Source Librarian can push data to the Central Librarian,
which can then push data to Destination A and Destination B. The Source
Librarian cannot push data directly to Destination A or Destination B; in fact
it does not even know that they exist!

Because of the push-based nature of the librarian, the two destinations have
complete control over how they want to manage the incoming data. Once it has
arrived, they can delete or modify the data as they see fit.

Forging the Connections
-----------------------

Each librarian will need a set of `background tasks <./Background.rst>`_ that
continuously manage ingress and egress of data. On the source, you will need
to have sending and queue management tasks, and on the destination, you will
need to have receiving tasks.

Before commiting the tasks and re-starting the server to load them, you will
need to provision accounts and associated librarians on each... Librarian.

Let's assume a two-librarian network (which, you will note, due to the push-based
system is how *all* networks are made: they are just simple combinations of various
two-librarian networks). There are two librarians here, with the name ``destination_librarian``
and ``source_librarian``.

Using the `provisioning tools <./Provisioning.rst>`, you will need to create
two accounts, one on each librarian:

- On the source librarian, create a user account with username ``destination_librarian``.
  Give this account Callback priviliges. You will need to note the password that you
  give this account down (``$destination_librarian_password``).
- On the destination librarian, create a user account with username ``source_librarian``.
  Give this account ReadAppend priviliges (``$source_librarian_password``).

Now, you will need to create a 'librarian' on each librarian. There are three main
command-line tools:

- ``librarian get-librarian-list $LIBRARIAN_NAME``, which fetches the currently
  connected librarians.
- ``librarian add-librarian $LIBRARIAN_NAME``, which adds a librarian
  to the list of connected librarians.
- ``librarian remove-librarian $LIBRARIAN_NAME``, which removes a librarian.

On the source librarian, you will need to add the destination librarian:

.. code-block:: bash

    librarian add-librarian $LIBRARIAN_NAME --name=destination_librarian 
                                            --url=$URL_OF_DESTINATION_LIBRARIAN
                                            --port=$PORT_OF_DESTINATION_LIBRARIAN
                                            --authenticator=source_librarian:$source_librarian_password

Note that the authenticator is the username and password on the librarian
you are connecting to, joined by a colon. This is encrypted in the database.

On the destination librarian, you will need to add the source librarian:

.. code-block:: bash

    librarian add-librarian $LIBRARIAN_NAME --name=source_librarian 
                                            --url=$URL_OF_SOURCE_LIBRARIAN
                                            --port=$PORT_OF_SOURCE_LIBRARIAN
                                            --authenticator=destination_librarian:$destination_librarian_password

Once these accounts are setup, you can begin the background transfers.

