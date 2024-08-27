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
and ``source_librarian`` (i.e. these are the names defined in the ``server_settings.json``).

Using the `provisioning tools <./Provisioning.rst>`_, you will need to create
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

No ``hl_config.json``?
-----------------------

However, most of the time you won't have access to a ``hl_config.json`` file
for the CLI client to work. In this case, you can use the python client to
create the librarians and accounts. Here is an example script that will
create the librarians and accounts for the above example:

.. code-block:: python

  """
  An example script for setting up a link between two librarians. You will need
  to run this both on the source and destination librarian.
  """

  from hera_librarian import AdminClient
  from hera_librarian.exceptions import LibrarianError, LibrarianHTTPError
  from hera_librarian.models.ping import PingResponse
  from hera_librarian.authlevel import AuthLevel
  import string
  import secrets

  import sys

  dry_run = "--dry-run" in sys.argv

  print(
      "Welcome to the librarian co-registration script. This will proceed in "
      "several steps. \n"
      "First, we will get administrator information about the librarian you are "
      "currently connected to.\n"
      "Second, we will create a new account on this librarian with either READAPPEND "
      "(destination) or CALLBACK (source) permissions.\n"
      "Between this step and the next, you should run this script on the other "
      "librarian to generate the appropriate account. \n"
      "Third, we will register the remote librarian. Be extremely careful to make sure that "
      "the user names are the same as the librarian names. \n"
      "You can run this script with --dry-run to see what it would do without actually doing it."
  )

  # Step 1: Get admin information
  print("\nStep 1: Getting administrator information.")
  admin_username = input("Enter the administrator username: ")
  admin_password = input("Enter the administrator password: ")
  librarian_host = input("Enter the librarian host (including http/https): ")
  librarian_port = input("Enter the librarian port: ")

  if not dry_run:
      try:
          client = AdminClient(
              host=librarian_host,
              port=int(librarian_port),
              user=admin_username,
              password=admin_password,
          )

          ping_response = client.ping()
      except (LibrarianHTTPError, LibrarianError):
          print("Failed to connect to librarian.")
          exit(1)
  else:
      ping_response = PingResponse(
          name="dry-run", description="A dry run librarian. Doesn't really exist."
      )

  print(
      "You are connected to librarian at {0}:{1}".format(librarian_host, librarian_port)
  )
  print(
      f"This librarian is called: {ping_response.name} (note that this is not "
      "the same as the user name you should use; see the config file for the librarian "
      "name)."
  )

  # Step 2: Create a new account
  print("\nStep 2: Creating a new account.")
  certain = False

  while certain is False:
      new_username = input(
          "Enter the new username (the 'name' of the librarian from the other script, printed above): "
      )
      new_password = "".join(
          secrets.choice(string.ascii_letters + string.digits) for i in range(32)
      )
      new_authlevel = getattr(
          AuthLevel,
          input("Enter the new Authlevel (READAPPEND or CALLBACK): ").upper(),
          None,
      )

      if new_authlevel not in [AuthLevel.READAPPEND, AuthLevel.CALLBACK]:
          print("Invalid AuthLevel.")
          continue

      yesno = input(
          "Are you certain you want to create a new account with "
          "these details?\n"
          f"Username: {new_username}\n"
          f"Password: {new_password}\n"
          f"AuthLevel: {new_authlevel}\n"
          "(yes/no): "
      )

      certain = yesno == "yes"

  if not dry_run:
      try:
          client.create_user(new_username, new_password, new_authlevel)
      except (LibrarianHTTPError, LibrarianError):
          print("Failed to create user.")
          exit(1)

  print(f"User created. Authenticator: {new_username}:{new_password}")

  print(
      "You should now repeat this process on the other librarian "
      f"to make a user with the name {ping_response.name}."
  )

  # Step 3: Register the remote librarian
  print("\nStep 3: Registering the remote librarian.")

  certain = False

  while certain is False:
      remote_librarian_host = input("Enter the remote librarian host (including http/https): ")
      remote_librarian_port = int(
          input("Enter the remote librarian port (443 for HTTPS default): ")
      )
      remote_librarian_name = input("Enter the remote librarian name: ")
      remote_librarian_authenticator = input(
          "Enter the remote librarian authenticator (from the remote invocation of this script, Step 2): "
      )

      if remote_librarian_name != new_username:
          print(
              "--------------------------------\n"
              f"WARNING: You used a different username ({new_username}) for the "
              f"remote librarian ({remote_librarian_name}) to the name you are using here. "
              "This is almost certainly the wrong thing to do. Continue at your own peril.\n"
              "--------------------------------"
          )

      if not ping_response.name == remote_librarian_authenticator.split(":")[0]:
          print(
              "--------------------------------\n"
              f"WARNING: The remote librarian authenticator ({remote_librarian_authenticator}) "
              f"does not contain the name of the librarian you are currently connected to ({ping_response.name}). "
              "This is almost certainly the wrong thing to do. Continue at your own peril.\n"
              "--------------------------------"
          )

      yesno = input(
          "Are you certain you want to register this remote librarian?\n"
          f"Host: {remote_librarian_host}\n"
          f"Port: {remote_librarian_port}\n"
          f"Name: {remote_librarian_name}\n"
          f"Authenticator: {remote_librarian_authenticator}\n"
          "(yes/no): "
      )

      certain = yesno == "yes"

  if not dry_run:
      try:
          client.add_librarian(
              name=remote_librarian_name,
              host=remote_librarian_host,
              port=remote_librarian_port,
              authenticator=remote_librarian_authenticator,
              check_connection=False,
          )
      except (LibrarianHTTPError, LibrarianError):
          print("Failed to add remote librarian.")
          exit(1)

  print("Remote librarian added.")
