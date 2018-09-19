# Version *next* (not yet released)

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
