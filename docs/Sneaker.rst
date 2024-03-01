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

Below, we have a step-by-step guide to performing a SneakerNet transfer.
