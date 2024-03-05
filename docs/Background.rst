Background Tasks
================

Background tasks are persistent tasks that run alongside the
librarian web server. They have access to the same database,
but they need not even run in the same container, and are
hence extremely flexible and scalable.

It is typical to run the background tasks in a separate
thread when running the librarian server (this is handled
automatically by the ``librarian-server-start`` script).
Most installations of the librarian will find that this is
more than suitable for their needs. For more complex scenarios,
where multithreaded web servers are required, may find it
useful to run the background tasks separately with the
``librarian-background-only`` script.

The background tasks are defined in the separate
``librarian_background`` package, and are configured
using a json file pointed to by ``$LIBRARIAN_BACKGROUND_CONFIG``.

Each background task is configured using a small json
object. The following two configurations are required,
for example, for a SneakerNet transfer:

.. code::json

    {
      "create_local_clone": [
          {
              "task_name": "Local cloner",
              "soft_timeout": "00:30:00",
              "every": "01:00:00",
              "age_in_days": 7,
              "clone_from": "store",
              "clone_to": "clone",
              "files_per_run": 256,
          }
      ],
      "recieve_clone": [
          {
              "task_name": "Clone receiver",
              "soft_timeout": "00:30:00",
              "every": "01:00:00",
              "files_per_run": 256,
          }
      ]
    }
            