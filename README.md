The HERA Librarian
==================

This system keeps track of all of the primary data products stored at a given
site. There is a Flask-based server that keeps track of everything using a
database and presents a nice frontend, and Python client code that can make
various requests of one or more servers.


Setting up a Server
-------------------

A Librarian installation requires:

1. A bunch of computers that store data (the “stores”)
1. A database server
1. A machine running the Librarian Flask server. This could potentially be
   one of the stores.

The server machine and store machines need to be able to SSH into each other
without passwords. The machine running the Flask server needs a Python
installation with the following modules:

1. [flask](http://flask.pocoo.org/)
1. [jinja2](http://jinja.pocoo.org/)
1. [sqlalchemy](http://www.sqlalchemy.org/)
1. [flask-sqlalchemy](http://flask-sqlalchemy.pocoo.org/)
1. [aipy](https://github.com/AaronParsons/aipy)
1. [numpy](http://www.numpy.org/)
1. Whichever database driver sqlalchemy will need to talk to your database server.

A standard Anaconda Python installation can provide all of these except
[flask-sqlalchemy](http://flask-sqlalchemy.pocoo.org/), which is easilly installable with `pip`.

To run a server, create a file in the `server/` subdirectory called `server-config.json`, using
`server-config.sample.json` as a template. There are a handful of values that need to be set.
Then, from that directory, run `./runserver.py`. That’s it!

The first time you run the server, you should temporarily modify the
configuration file to initialize the database and create entries describing
the stores present at your site.


Client tools
------------

Besides the server, this repository provides a Python module,
`hera_librarian`, that lets you talk to one *or more* Librarians
programmatically. Documentation not yet available. Install it with

```
python setup.py install
```

in this directory. This also provides a few scripts:

* `add_obs_librarian.py` — Meant to be run on a store computer; notifies a
  Librarian of new files that it ought to be aware of.
* `librarian_launch_copy.py` — Instruct one Librarian server to start copying
  a file to another Librarian.
* `upload_to_librarian.py` — Uploads a file to a Librarian. If the origin
  file is already known to a different Librarian, `librarian_launch_copy.py`
  should be used to preserve metadata.
