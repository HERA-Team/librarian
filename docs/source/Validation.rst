Validating Files
================

Unlike rule-based systems, the Librarian is managed much more manually.
This means that at some times you may wish to check how many copies of
files are available in the system, and whether they are still valid
(e.g. have the correct checksum)!

The Librarian contains tools to perform these duties, and they do not
happen automatically. It is strongly recommended that you validate
files before deleting them from e.g. a storage-limited source node.


Using the Python Client
-----------------------

The python client can be used to validate files:

.. code-block::python

    client: LibrarianClient

    response = client.validate_file(
        file_name="my_favourite_file/at_a_path/hello"
    )

Here response should be a list of objects. These objects contain
information **that you can use yourself to determine whether there
are enough valid copies of the file in the network for your purposes**.
Because different users have potentially wildly different needs,
you will need to check the response objects. These are
``FileValidationResponseItem``s that contain the following parameters:

- ``librarian`` - the name of the librarian that this file is stored on.
- ``store`` - the ID of the store that the file lives on
- ``instance_id`` - the local instance ID of the file copy
- ``original_checksum`` - the database-provided checksum that was
  used at file upload time.
- ``current_checksum`` - the current checksum of the file that has been
  re-computed from the bytes on disk.
- ``computed_same_checksum`` - a boolean telling you whether this particular
  instance has the same checksum as is stored in the database.

There is also sizing information for the files stored here.


Using the Command-Line Client
-----------------------------

You can use the command-line client to validate files too:

.. code-block::

    librarian validate-file local test.txt

    Checksum Match | Current Checksum                       | Librarian   | Original Checksum               
    -------------- | -------------------------------------- | ----------- | --------------------------------
    True           | md5:::440d5758b601be7fbee75ae3d41c7262 | live_server | 440d5758b601be7fbee75ae3d41c7262