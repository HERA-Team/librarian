Librarian
=========

The librarian is a tool for data transfer orchestration. The librarian uses a unified,
global, namespace for file storage (i.e. filenames are unique across the entire system),
with a core assumption that once data is entered into the librarian it is immutable.
The librarian is made up of four main parts:

- The librarian client, use for communicating with the server, to ingest and search
  for files (along with many other things).
- The librarian database server, which is any SQL-like database that can be used
  with basic SQLAlchemy features. This database stores information like the names and
  sizes of files that have been ingested into the librarian.
- The librarian HTTP server, which is a FastAPI server that communicates with the
  client (and other librarian servers) to pass metadata about files around. Data is
  ingested using various (non-HTTP-based) methods dependent on availability.
- The librarian background tasks. These tasks run in a separate thread (or even
  on a different server, depending on deployment details) and are responsible for
  things like checking the integrity of files, recieving and sending clones to
  other librarians or drives, and any other recurring task that needs to be performed.