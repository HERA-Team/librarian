Uploading to the Librarian
==========================

The most important client operation for the librarian is uploading.
In most settings, this will be done by a single client that is connected
to the end of the data creation pipeline. We strongly recommend only
having one librarian in a given network be an ingest point to avoid
unnceessary conflicts, with all data flowing down the 'waterfall'
of inter-connected librarains.

It is generally recommended that you have a separate 
`user <Provisioning.rst>`_ for the uploader, with only ReadAppend
permissions.

The `upload` command is used to upload data to the librarian. The
command takes a single argument, which is the path to the file to
upload. The file will be uploaded to the librarian, and the command
will block until the upload is complete.

.. code-block:: bash
    
    librarian upload $LIBRARIAN_NAME /path/to/file name/on/librarian/file

There are two important paths here: the first one (the source path), and
the second one (the destination path). The first one is the path to the
file on your machine, and can be whatever you like. It can be a path to
a single file, or a whole directory you would like to upload.

The second path, the destination path, is the path that the file will be
stored under on the librarian. This path is relative to the root of the
store area. Designing a good strategy for your filenames is important,
but is left to the user. The librarian will not overwrite files, so if
you upload a file with the same name as an existing file, the upload
will fail.

*The name of your file is important*. The name is permanent, unique, and
*unchangeable*, so once you upload a file you're stuck with it. Names
must be unique across the entire librarian system.

Python Client
-------------

Alternatively, and more likely in practice, you can use the `upload`
directive in the python client to upload data.

.. code-block:: python

    from hera_librarian import LibrarianClient
    from hera_librarian.settings import client_settings
    from pathlib import Path

    conn = client_settings.connections.get(
        "my_librarian_name"
    )

    conn.upload(
        Path(local_file),
        Path("/hello/world/this/is/a/file.txt")
    )

