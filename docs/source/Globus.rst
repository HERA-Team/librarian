Globus Connectivity
===================

When you are not forced into using SneakerNet functionality, you will 
want to move files over the internet. This can be accomplished by hooking
into Globus, a high-speed data transfer service.

This guide assumes you have:

- Two librarians already set up and ready to transfer files.
- Two globus endpoints, one at the source librarian and one at
  the destination librarian.
- A globus account that can transfer files between those two
  endpoints.

The transfers between these two librarians will be automated. As such,
it is recommended that you set up some observability of your librarians
before you begin. This is how you will monitor your transfers.

A crucial quirk of the librarian system is that your globus endpoint
must have the exact same absolute filesystem structure as the one that
the libraria sees. For example, a path like ``/my/file/location`` must be
``/my/file/location`` on the globus endpoint too; no globus roots are allowed.

Inter-librarian transfers happen asynchronously. In an A to B transfer:

- Within the ``send_clone`` task:
  + A calls up B to batch-stage up to ``N`` files.
  + B creates ``N`` staging directories.
  + B responds with the globus endpoint ID to send files to.
  + A creates an item in the 'send queue' linked to these transfers.
- Within the ``consume_queue`` task:
  + A picks up all available 'send queue' tasks.
  + Each task is shipped off in a single globus transfer (i.e. each transfer
    is responsible for sending up to ``N`` files).
  + Up to ``M``, which is set to not go over the globus-imposed limit of 100,
    globus tasks can be active in the globus-managed queue at once.
- Within the ``check_consumed_queue`` task:
  + Each active globus task is checked to confirm its status. Transfer of
    actual bytes is handled by globus running in a separate process.
  + If the globus task is complete, A calls up B and marks all files
    as STAGED.
- Within the ``recv_clone`` task:
  + Once the incoming files are staged, B ingests them.
  + B calls back to A to register a 'Remote Instance' of each file.


Required Variables
------------------

To enable globus connectivity, on the source side you will need
to provide (in the server settings JSON config):

- ``globus_enable`` needs to be set to ``true``
- ``globus_client_id`` needs to be set to the UUID for your globus account
  with associated privilges.
- ``globus_client_native_app``: a boolean describing whether or not to use
  a Native App (``true``) or a Confidential App (``false``) for the client. Usually,
  the Native App is used (set this to ``true``).
- ``globus_local_endpoint_id``: the endpoint UUID of the source librarian.
- ``globus_client_secret_file``: your authorization API key from globus.

On the destination side, you will also need to create a configuration for
an asynchronous transfer manager in your destination store:

.. code-block::json

  {
    ...
    "add_stores": [
      {
        "store_name": "globus_example",
        "store_type": "local",
        "ingestible": true,
        "store_data": {
          ...
        },
        "asynchronous_transfer_manager_data": {
          "globus": {
            "available": true,
            "destination_endpoint": "ACBD-2232-232323...."
          }
        }
      }
    ]
  }

All that is required here is the ``destination_endpoint`` which is the
UUID of the globus endpoint for the destination librarian.


Required Background Tasks
-------------------------

At a minimum, you will need ``send_clone``, ``consume_queue``, and 
``check_consumed_queue`` on the source side, and ``recv_clone`` on the
destination side. More information on background tasks is available on
the appropriate `page <./Background.rst>`_.


Hypervisors
^^^^^^^^^^^

Sometimes things go wrong. They *will* go wrong if you are running
applications on the internet. Every now and then a callback will fail,
or a transfer will get stuck... To automatically deal with these
problems, we have 'hypervisors'. There are two types of hypervisors:

- ``incoming_transfer_hypervisor``: ran on the destination side,
  and takes the usual parameters plus ``age_in_days``.
- ``outgoing_transfer_hypervisor``: ran on the source side,
  and takes the usual parameters plus ``age_in_days``.

If an incoming or outgoing transfer passes the age specified here,
a call to the opposing librarian is made to query the status. If
there is a mis-match, the problem is handled gracefully. It is strongly
recommended that you let outgoing transfers age out sooner (i.e. ``age_in_days``
is less) than incoming transfers, as with the push-based librarian it 
is easier to handle things gracefully in this way.

The most common case, for example, is when a callback from B to A
fails because of network interruption. Here, the outgoing transfer
hypervisor will find it (as it will still be STAGED on A), call
up B, find that the instance exists, and register a remote instance
on A. The transfer is then marked as complete.


Enabling and Disabling Transfers
--------------------------------

There may be points in time when you want to shut down transfers to
specific machines. Whilst this can always be performed by editing the
configuration files and restarting the server, that is not always optimal.

Instead, you can use the following command-line script inside a container
(i.e. with direct access to the database):

.. code-block::

   librarian-change-transfer-status [-h] --librarian LIBRARIAN [--enable] [--disable]

   Change the status of an external librarian, to enable or disable transfers.

   options:
     -h, --help            show this help message and exit
     --librarian LIBRARIAN
                           Name of the librarian to change the status of.
     --enable              Enable the librarian.
     --disable             Disable the librarian.

Or using the client:

.. code-block::

   librarian get-librarian-list [-h] [--ping] CONNECTION-NAME

   Get a list of librarians known to the librarian.

   positional arguments:
     CONNECTION-NAME  Which Librarian to talk to; as in ~/.hl_client.cfg.

   options:
     -h, --help       show this help message and exit
     --ping           Ping the librarians to check they are up.


to find information about the connected librarians, and to set their properties:

.. code-block::
   
   librarian set-librarian-transfer [-h] [--name NAME] [--enabled] [--disabled] CONNECTION-NAME

   Set the transfer state of a librarian.

   positional arguments:
     CONNECTION-NAME  Which Librarian to talk to; as in ~/.hl_client.cfg.

   options:
     -h, --help       show this help message and exit
     --name NAME      The name of the librarian to set the transfer state of.
     --enabled        Set the librarian to enabled for transfers.
     --disabled       Set the librarian to disabled for transfers.


These client tools require an administrator account to use.