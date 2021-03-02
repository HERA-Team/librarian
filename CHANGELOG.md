# Version *next* (not yet released)


# Version 1.2.0 (2021 Jan 25)
- Add support for running inside a Docker container.


# Version 1.1.1 (2020 Feb 17)
- Fix handling of directories in Globus.


# Version 1.1.0 (2020 Feb 7)
- Add support for transfers using Globus as well as associated documentation.
- Add `librarian check-connections` command to test connectivity.
- Improve documentation of the connectivity model.
- Fix background tasks in python3. This adds multiple background
  worker thread functionality again.


# Version 1.0.3 (2019 Oct 22)

- Make background tasks work in python3. For now this comes at the
  expense of multiple background worker threads.
- Fix distribution so server works from system-wide installation.


# Version 1.0.2 (2019 Oct 6)

- Fix server versioning for tagged releases.


# Version 1.0.1 (2019 Oct 6)

- Use setuptools_scm for versioning.
- Add documentation for quickly setting up a librarian server.


# Version 1.0.0 (2019 Sep 19)

- Convert codebase from python2 to python3.
- Move scripts to `cli.py` module and replace with single command.
- Reorganize repo structure.
- Add tests and CI support.
- Support automatic ingestion of uvh5 files.
- Avoid potential problems with SQLAlchemy when using a multiprocess
  server.


# Version 0.1.6 (2018 Sep 19)

- Fix programmatic searches for file instances.
- Don't hard-require `aipy` inside `get_obsid_from_path`.
- Bugfix: stop having staging fail when attempting to stage multiple
  instances of a directory.


# Version 0.1.5 (2018 Aug 14)

- Merge in the `multiprocess` branch — the Librarian now runs multiple
  servers simultaneously. This change hasn’t been as thoroughly reviewed
  as I’d like, but it's been running in production for months now, so
  we should really merge it in.
- Tidy up the `setup.py` and related infrastructure, and start providing
  releases on PyPI.
- Document the `--null-obsid` option to `upload_to_librarian.py`.


# Version 0.1.4 (2018 Jan 4)

- Fix “staging” when the Librarian is configured to change ownership of
  files after the staging completes.


# Version 0.1.3 (2017 Dec 18)

- Add the “staging” feature, allowing users to launch transfers of files
  from the Librarian internal storage to Lustre at NRAO.
- Added a Python API for searching.
- Make it possible to create files with null obsids. This is intended for
  maintenance files not directly associated with telescope data.
- Protect all database commits with rollbacks. Hopefully this will increase
  reliability in the face of intermittent database errors.


# Earlier versions

Change history not documented except in Git.
