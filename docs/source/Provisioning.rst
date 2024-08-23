Provisioning Users
==================

Provisioning users can be performed using the python client or
the command-line client.

New users can be created using the command-line client as follows:

.. code-block:: bash

    librarian create-user {LIBRARIAN} --username $USERNAME --password $PASSWORD --auth-level $AUTH_LEVEL

The ``$AUTH_LEVEL`` is a string that corresponds to one of the levels
in the ``AuthLevel`` enum,

- ``NONE``: No authentication is provided. Can only ping the server.
- ``READONLY``: Only read access is provided.
- ``CALLBACK``: Only read and extremely limited callback access is provided.
  These users are generally used for downstream librarians that use
  their callback privliages to register remote instances.
- ``READAPPEND``: Read and append privliages, typical for accounts
  that are used to ingest data into the librarian. These users can add
  new files, but not remove existing ones.
- ``READWRITE``: Full read and write access. These users can add, remove,
  and modify files in the librarian.
- ``ADMIN``: Full access, including librarian configuration management.

Once a user is created, it can be deleted as follows:

.. code-block:: bash

    librarian delete-user {LIBRARIAN} --username $USERNAME