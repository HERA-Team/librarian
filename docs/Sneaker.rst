SneakerNet Transfers
====================

Privilege Level Required: Administrator.
Version: 2.1.0 and above.
Database Requirement: ``d8934c52bac5``

Introduction
------------

SneakerNet transfers are asynchronous transfers that can be managed
by the librarian. A SneakerNet transfer is a transfer of data from one
site to another using human power; i.e. the data is moved by hand
(for instance, on a USB stick, hard drives, etc.).

A generic SneakerNet transfer occurs in the following steps:

- A clone of a sub-set of data is created on an external device.
- A manifest of the data is created.
- The data is physically transferred to the destination site.
- The manifest is used to ingest the data into the destination site.
- A callback from the destination to the source occurs to confirm the
  transfer has completed successfully.

Specifically within the librarian, SneakerNet transfers have the
following steps:

- If this is the first time using SneakerNet, add a new store to the
  librarian that represents the device you would like to use to
  SneakerNet data. If the store already exists, make sure it is
  enabled using the administrator endpoint ``set_store_state``.
- Set up a ``CreateLocalClone`` background task on the source
  librarian to create a copy of the data to be transferred.
- Register the remote librarian with the source librarian and
  vice-versa.
- Use the ``get_store_manifest`` client operation to create a
  manifest of the cloned store. There are a few helpful options
  here: ``create_outgoing_transfers`` creates an ``OutgoingTransfer``
  object for each file in the store to the ``destination_librarian``,
  ``disable_store`` disables the store on the source librarian before
  generating the manifest (to ensure no new data is added to the store
  and to allow the device to be swapped out), and
  ``mark_local_instances_as_unavailable`` marks all instances of
  the file on the new store as unavailable.
- This store manifest can then be saved to the device to be moved
  along with the data. It is recommended that you back up (and
  potentially version control) the manifests.
- Move the device to the destination site.
- Use the ``ingest_store_manifest`` client operation to ingest the
  data into the destination librarian. At this point, the data is
  only staged on the librarian, and is not yet available on the
  store or to users.
- The ``RecieveClone`` background task on the destination librarian
  will create a ``File`` and ``Instance`` for each file in the store.
  Afterwards, the destination librarian will use its database
  entry for the source librarian to callback. As part of processing
  this callback, the source librarian will mark its ``OutgoingTransfer``
  as complete and create a ``RemoteInstance`` for each file that
  has been successfully transferred.

Below, we have a step-by-step guide to performing a SneakerNet transfer using
the librarian command-line interface.

Step 1: Adding or enabling a store
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For more information on adding a store, see :ref:`Stores`. It is crucial
to mark SneakerNet stores as 'non-ingestible' (i.e. set ``ingestible: false``
in the configuration file), otherwise they themselves will ingest new
data passed to the librarian.

There are three main states that are important for stores:

1. ``ingestible``: Whether or not 'fresh' files (those sent from uploads
   or from clones) can be added to the store.
2. ``enabled``: Whether or not the store is currently marked as available
   for use. All stores start out enabled, but may be disabled when they
   are full, or a disk is being swapped out.
3. ``available``: This is an internal state that is tracked, irrespective
   of ``ingestible`` or ``enabled`` which indicates whether the physical
   device is available for recieving commands. For local stores, this is
   generally forced to be true.

If your store is starting out disabled, you will need to enable it
by using the ``set_store_state`` endpoint. This can be easily accomplished
using the command-line utility:

.. code:: bash

    $ librarian set-store-state local-librarian --store local-store --enabled
    Store local-store state set to enabled.

This sets a store called ``local-store`` on a librarian (as defined in
``~/.hl_config.cfg``) to be enabled. If the store is already enabled, this will
still go through.

If you need to know what stores are available on the librarian, you can use
the following command-line wrapper to ``get_store_list``:

.. code:: bash

    $ librarian get-store-list local-librarian
    local-store (local) [599.5 GB Free] - Ingestable - Available - Enabled

Which will print out helpful information about all attached stores to the
librarian. As these things are generally meant to be transparent to regular
users of the librarian, these endpoints require administrator privileges.

Step 2: Background tasks and remote librarians
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are two core background tasks that are used in SneakerNet transfers:
``CreateLocalClone`` and ``ReceiveClone``. The first is used at the source site
to create a complete clone of the data ingested into the librarian, and the
latter is used to ingest the data into the destination librarian. More
information on background task scheduling is available in the :ref:`Background`
section.

At each librarian site, you will also need to register the remote librarian
using the command-line tools. This will also generally involve account
provision on both librarians, as callbacks are required.

To provision a new account, you will need to use the ``create_user``
endpoint, which can be accessed through the command-line tool:

TODO: THIS SHOULD BE COMPLETED IN RESPONSE TO ISSUE #61.

Once the appropriate accounts are provisioned, you will need
to register them with their respective librarians. This can be done
with the ``register_remote_librarian`` endpoint:

TODO: THIS SHOULD BE COMPLETED IN RESPONSE TO ISSUE #60

Step 3: Creating a store manifest
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once one of your SneakerNet stores are filled up, you can create
a manifest of the store using the ``get_store_manifest`` endpoint.
This process will also disable the store on the source librarian,
create outgoing transfers, and mark local instances as unavailable,
ready for the disk to be replaced.

.. code:: bash

    $ librarian get-store-manifest local-librarian --store local-clone --create-outgoing-transfers --disable-store --mark-instances-as-unavailable --output /path/to/manifest.json

The file will be saved as a serialized json object. It is strongly
recommended that you back up this file, as it is the only unique
record of the data that is being transferred. It should also likely
be packaged with the SneakerNet transfer for easy ingestion on
the other side.

Step 4: Moving the data
^^^^^^^^^^^^^^^^^^^^^^^

You will then need to move the data to the destination site. This
is generally done by physically moving the device to the destination
site. It is recommended that you also move the manifest file with
the data, as it will be required for the next step, as well as
sending this (considerably smaller amount of data) over the network.

Step 5: Ingesting the store manifest
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the data has been moved to the destination site, you will need
to ingest the data into the librarian. This is done using the
``ingest_store_manifest`` endpoint: