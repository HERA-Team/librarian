Stores
======

Stores in the librarian are abstractions over various storage systems.
The most common of these is a local POSIX filesystem, but there is
a completely abstract system within the librarian for interacting with
any generic storage system (such as a block or key-value store).

Because stores are such a foundational part of the librarian, they are
defined in the configuration file, as pointed to by
``$LIBRARIAN_CONFIG_PATH``.

The configuration file is a JSON file, and the stores are defined in the
``add_stores`` key. The value of this key is a dictionary that is
de-serialized to a ``StoreMetadata`` class. In the same block, you will
need to defined the transfer managers that can be used for data
ingress and egress. Documentation on how to configure the stores
is available under the stores themselves, and the transfer managers
likewise.

More documentation will be added in the future noting how to set up
the asynchronous transfers and details of transfer managers.

.. code:: json
    "add_stores": [
        {
            "store_name": "store",
            "store_type": "local",
            "ingestable": true,
            "store_data": {
                "staging_path": "/tmp/store/libstore/staging",
                "store_path": "/tmp/store/libstore/store",
                "report_full_fraction": 1.0,
                "group_write_after_stage": true,
                "own_after_commit": true,
                "readonly_after_commit": true
            },
            "transfer_manager_data": {
                "local": {
                    "available": true,
                    "hostnames": [
                        "compute-0.0.local",
                        "example-librarian-hostname"
                    ]
                }
            }
        }
    ]

