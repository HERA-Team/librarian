The Librarian Client
====================

The librarian client really refers to two individual components:

- The python API defined in ``hera_librarian/client.py``, which is 
  two well-documented objects ``LibrarianClient`` and ``AdminClient``
  that provide wrappers over the REST API.
- The ``librarian`` command-line tool that passes through to this
  client.

Of course, should you wish to access the librarian server from
outside of python or the command line, you could always make the
REST calls yourself using any appropriate tool. The benefit of using
the python interface is that all data is transformed on the
client-side using the same pydantic models (``hera_librarian/models``)
that are used on the server side, providing significant client-side
validation and error checking that may be useful to you.

Python client
-------------

The librarian python client is easy to use, and is well-documented
through its docstrings. The ``LibrarianClient`` object contains
all the methods that can be used without administration privliages
on the server, and the ``AdminClient`` contains methods that wrap
all endpoints. Using the client is simple:

.. code-block::python

    from hera_librarian import LibrarianClient
    from datetime import datetime, timedelta

    client = LibrarianClient(
        host="localhost",
        port=12345,
        user="me",
        password="password"
    )

    # Grab all files that have been uploaded in the past week

    results = client.search_files(
        create_time_window=[
            datetime.datetime.utcnow() - timedelta(days=1),
            datetime.datetime.utcnow()
        ],
    )

    for result in results:
        print(result.filename)


The python interface is considered stable (appendable), and is the 
preferred way for programatically interacting with the librarian server.

Commad-line client
------------------

The command-line client wraps almost all of the python functions
through the use of the ``argparse`` library. The command-line
library can be somewhat clunky due to the large number of parameters
that are needed for most functions on the librarian, and as such
its use is generally discouraged for anything other than simple
interactions.

To use the command-line client, you will need to provide information
for the server you are connecting to through a JSON parameter file.

.. code-block::json

    {
      "connections": {
        "local": {
          "host": "localhost",
          "port": 12345,
          "user": "me",
          "password": "password"
        },
        "local-admin": {
          "host": "localhost",
          "port": 12345,
          "user": "admin",
          "password": "super-secret"
        }
      }
    }

This file is generally stored at ``~/.hl_client.cfg`` (we recommend using 400
permissions for this file!), but its location can be configured through
the use of the ``HL_CLIENT_CONFIG`` environment variable.

A list of available commands is availble through the ``librarian --help`` command.
Command-line interactions generally take the following form:

.. code-block::

    librarian {command} {connection-name} {parameters}


For instance, to search for files that have been uploaded in the past week
on the local server, you would use the following command:

.. code-block::
    
    librarian search-files local --create-time-start=$(date -uv -1d) --create-time-end=$(date -u)

Individual parameter information is available through 
``librarian {command} --help``.


