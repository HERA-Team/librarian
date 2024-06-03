# The Librarian

[![Coverage Status](https://coveralls.io/repos/github/simonsobs/librarian/badge.svg?branch=main)](https://coveralls.io/github/simonsobs/librarian?branch=main)

The Librarian is a system for data transfer orchestration designed for the
Simons Observatory. The Librarian is based upon the HERA Librarian framework
(and even retains its namesake for the client library), but was entirely
re-written for this new workload. The Librarian's architechture is documented
in the Scipy 2024 proceeding describing the software.

The goal of the Librarian is to track and transfer primary data products
generated at remote sites through a push-based system. The first push is
from a client using the `librarian upload` command, ingesting files into
the system. From there, redundant copies may be made locally or remotely,
with support for SneakerNet (i.e. by movement of physical media) transfers.

There are a number of sub-packages in this repository:

- `hera_librarian`, containing the client code for the system. This is how
  most users communicate with the librarian.
- `librarain_server`, containing the main HTTP REST API server and database
  models.
- `librarian_background`, containing the code for background tasks that
  run on a fixed cadence, and have direct database access.
- `librarian_server_scripts`, containing the initialization and example
  run scripts that are used for the persistent service.
- `tests` containing a large suite of tests for the application.
- `alembic` containing database migration scripts.

These individual components are all documented separately. 