The HERA Librarian Server
=========================

This directory contains tools needed to run the Librarian server.

The bulk of the code is contained in a module named `librarian_server` located
in this directory. It is installed alongside the `hera_librarian` client module,
and the additional dependencies required to run the server can be installed
using
```
pip install .[server]
```
from the top level of the repo.

The server state is backed by a database. We use
[Alembic](http://alembic.zzzcomputing.com/) to manage the evolution of the
database schema.


Setting up a Server
-------------------

A Librarian installation requires:

1. A bunch of computers that store data (the “stores”)
1. A database server
1. A machine running the Librarian web server. This could potentially be
   one of the stores or the database server.

The server machine and store machines need to be able to SSH into each other
without passwords. The machine running the web server needs a Python
installation with the following modules:

1. [flask](http://flask.pocoo.org/)
1. [jinja2](http://jinja.pocoo.org/)
1. [sqlalchemy](http://www.sqlalchemy.org/)
1. [flask-sqlalchemy](http://flask-sqlalchemy.pocoo.org/)
1. Whichever database driver sqlalchemy will need to talk to your database server.
1. [aipy](https://github.com/HERA-Team/aipy)
1. [numpy](http://www.numpy.org/)
1. [astropy](http://www.astropy.org/)
1. [tornado](http://www.tornadoweb.org/) optionally, for robust HTTP service
1. [alembic](http://alembic.zzzcomputing.com/)
1. [pyuvdata](https://github.com/RadioAstronomySoftwareGroup/pyuvdata)
1. [pytz](http://pytz.sourceforge.net/)

A standard Anaconda Python installation can provide all of these except
`aipy` and `pyuvdata`. These are available through `pip` or the `conda-forge`
channel, like so:
```
conda install -c conda-forge aipy pyuvdata
```

To set up a new Librarian server:

1. Create a database for the Librarian using the system of your choice. The
   on-site system uses [Postgres](https://www.postgresql.org/) so this will
   always be the best-supported option.
1. Create a file called `server-config.json`, using `server-config.sample.json`
   as a template. There are a handful of values that need to be set — most
   importantly, the [SQLAlchemy database
   URL](http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls)
   needed to talk to your database. Once this file is created, you **must**
   export its full path to the `LIBRARIAN_CONFIG_PATH` shell variable.
1. From the top level directory, run `alembic upgrade head` to initialize the
   database schema using Alembic’s infrastructure.
1. Finally, run `runserver.py` to boot the server.


Updating the Database Schema
----------------------------

When in the course of human events it becomes necessary for a developer to
change the schema of the Librarian database, the developer must use Alembic to
manage the schema evolution. The workflow for *specifying* a schema change is
this:

1. Read [the Alembic documentation](http://alembic.zzzcomputing.com/) to
   familiarize yourself with how it works.
1. Change the schema in the files in the `librarian_server` module as needed.
1. In the main repo directory, run `alembic revision --autogenerate -m
   $DESCRIPTION_OF_CHANGE`, where `$DESCRIPTION_OF_CHANGE` is a terse
   description of the change to the schema. Alembic will do its best to figure
   out what you did to the schema and record the changes in a new file in the
   subdirectory `alembic/versions`.
1. **Review and edit that new file** to make sure that it makes sense, provides
   default values for new columns as needed, etc.
1. Add the new file to Git and commit it with your schema change. Ideally the
   commit introducing the changed schema should change nothing else about the
   Librarian.
1. Use the Docker-based test rig to verify that everything works.

The workflow for *deploying* a schema change on a given Librarian instance is:

1. Shut down the running Librarian server.
1. Use Git to pull in a version of the codebase with the changed schema.
1. From the main repo directory, run `alembic upgrade head` to update the
   database schema to the newest version. You may need to set the environment
   variable `LIBRARIAN_CONFIG_PATH` to point to the Librarian server
   configuration file if it has not already been set.
1. Restart the Librarian server.

Obviously, you should not deploy a schema change to one of the production
servers (on-site, NRAO) until you are sure that the associated change is one
that we want to commit to.


Quick and Dirty Guide for Installing and Testing a Librarian Server
===================================================================

Here is a quick introduction for how to stand up a librarian server and upload
a file to it. We assume the user is using Postgres as the backing database,
and is running on Ubuntu 18.04.

1. Install the postgres package:
   ```
   sudo apt update
   sudo apt install postgresql postgresql-contrib
   ```
1. Set up postgres to work with the testing database. Note the name of the
   database is defined in the server config file. This database name matches the
   one defined in [the sample config file](../ci/server-config-ci.json):
   ```
   sudo su postgres
   psql -c "create database librarian_test;"
   psql -c "create user <your_username>;"
   exit
   ```
1. Clone the librarian repo:
   ```
   git clone https://github.com/HERA-Team/librarian.git
   ```
1. Install the librarian package and dependencies:
   ```
   cd librarian
   pip install .[server]
   ```
1. Point to the testing server config file:
   ```
   export LIBRARIAN_CONFIG_PATH=`pwd`/ci/server-config-ci.json
   ```
1. Use `alembic` to update the database schema:
   ```
   alembic upgrade head
   ```
1. Copy the high-level client config file to the proper location:
   ```
   cp ci/hl_client.cfg ~/.hl_client.cfg
   ```
1. Make a scratch directory for the librarian to use as a "store". These are
   defined in the `add-stores` section of the server config file:
   ```
   mkdir /tmp/librarian
   ```
1. Launch the librarian server:
   ```
   runserver.py
   ```
1. In a separate terminal, attempt to upload a file to the running librarian
   server:
   ```
   librarian upload --null-obsid TestUser README.md foo/README.md
   ```
1. Verify that the file was successfully uploaded to the librarian. In a web
   browser, navigate to `localhost:21108`. You should use the authenticator
   string `I am a human`. After authenticating, you should see `README.md`
   listed under "Most Recent Files". Success!
