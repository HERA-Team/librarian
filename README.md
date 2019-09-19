# The HERA Librarian

This system keeps track of all of the primary data products stored at a given
site. There is a Flask-based server that keeps track of everything using a
database and presents a nice frontend, and Python client code that can make
various requests of one or more servers.

All of the server code is in a subdirectory called `server`. See
[the README there](librarian_packages/server/README.md) for
information on how to run a server.

Besides the server, this repository provides a Python module,
`hera_librarian`, that lets you talk to one *or more* Librarians
programmatically. Documentation not yet available. Install it with
[pip](https://pip.pypa.io/en/stable/) or

```
python setup.py install
```

in the `hera_librarian` subdirectory.


## Documentation

See [here](docs/Index.md).


## Change Log

See [CHANGELOG.md](./CHANGELOG.md) for a summary of the changes in each
released version.
