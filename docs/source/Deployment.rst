Deployment
==========

Deploying the librarian is a relatively simple task thanks to containerisation.
We currently *strongly* recommend that librarian instances are deployed using
Docker, rather than running on 'bare metal'. You will need at least two containers.

1. A container running the librarian application
2. A container running a SQLAlchemy-compatible database (we recommend PostgreSQL)

There are several additional containers that you may wish to consider:

1. A reverse-proxy container (e.g. Nginx) to handle SSL termination.
2. A container running a caching layer (e.g. Redis) to improve performance (rarely used).
3. A container running Grafana to display dashboards.

The following sections will guide you through the process of deploying the librarian
using Docker.

The Server Config
-----------------

The server configuration is a JSON file that contains all the configuration
information required to run the librarian. This should be placed in a directory
accessible to the librarian; we recommend running this as either a config map
or a bind mount so that it can be easily changed.

The server configuration file should be pointed to as the ``LIBRARIAN_CONFIG_PATH``
environment variable. All of the below configurations can be modified using
environment variables with ``LIBRARIAN_SERVER_`` as a prefix.

The configuration variables are as follows:

- ``name``: The name of the server. This is used in the logs and in the inter-librarian
  transfers, so should be unique within your librarian setup. When other librarians register
  this as a potential downstream or upstream node, the name should be the same as this.
- ``debug``: Whether to run the server in debug mode. This should be set to
  ``false`` in production. With debug on, the server will make available API docs
  and other potentially sensitive information.
- ``encryption_key_file``: The path to the file containing the encryption key. This should
  be a Fernet key and is used for encrypting and decrypting the data stored
  about other librarians in the database.
- Database settings:

  * ``database_driver``: The database driver to use. This should be one of the
    SQLAlchemy-supported database drivers. We recommend ``postgresql+psycopg``.
  * ``database_user``: The username to use to connect to the database.
  * ``database_password``: The password used to connect to the database.
  * ``database_host``: The hostname of the database.
  * ``database_port``: The port of the database.
  * ``database_name``: The name of the database.
  * ``alembic_config_path``: The path to the alembic configuration file. This
    is used to run database migrations.
  * ``alembic_path``: The path to alembic (defaults to ``alembic``), in case it
    is for some reason outside of the PATH.
- ``log_level``: The log level to use. This should be one of the Python logging
  levels. We recommend ``INFO`` for production.
- ``displayed_site_name``: A pretty name for the site, this is used in user
  interfaces like the slack hooks.
- ``displayed_site_description``: A description of the site, this is used in user
  interfaces like the slack hooks.
- ``host``: The host to bind to. This should be ``0.0.0.0``. 
- ``port``: The port to bind to.
- ``add_stores``: A JSON object containing the information about stores added
  to the ilbrarian. See `stores <./Stores.rst>`_.
- ``max_search_results``: The maximum number of search results to return to clients.
  Not respected for administative users.
- ``maximal_upload_size_bytes``: The maximum size of a file that can be uploaded
  to the librarian. This is in bytes. By default, this is 50 GB.
- Slack integration:

  * ``slack_webhook_enable``: Whether to use the slack hook.
  * ``slack_webhook_url_file``: The path to the file contianing the
     URL of the Slack webhook to send messages to.
  * ``slack_webhook_post_error_severity``: Which error severities to post to slack.
    By default, all errors are sent.
  * ``slack_webhook_post_error_category``: Which error categories to post to slack.
    By default, all errors are sent.

In addition to this server configuration, you will need an appropriate background
configuration for this server. More information can be found on the
`background <./Background.rst>`_ page.

It is recommended that you store your secrets files in the same directory as the
configuration files for ease of use.

The Database
------------

We strongly recommend using a PostgreSQL database for the librarian, and
the librarian is tested in a production setting using postgres 16. You will need
to export the port of the database so that it is visible to the librarian server.

An example configuration for a PostgreSQL database is as follows (docker compose):

.. code-block:: yaml

    services:
      librarian-database:
        image: postgres:16
        restart: always
        container_name: "librarian-database"
        volumes:
          - type: "bind"
            source: "/librarian/database"
            target: "/var/lib/postgresql/data"
        environment:
          POSTGRES_USER: "librarian"
          POSTGRES_PASSWORD: "hello-world-password"
          POSTGRES_DB: "librarian"
        expose:
          - 5432
        networks:
          - "librarian-network"
        healthcheck:
          test: ["CMD-SHELL", "pg_isready"]
          interval: 10s
          timeout: 10s
          retries: 5

    networks:
      librarian-network:

Note that you will need to set up your own volume or bind mount and backups for
this container. Here we use the example of a bind mount to a directory called
``/librarian/database``.

The Librarian
-------------

The librarian itself is a relatively simple container, coming with a docker file
in this repository that simply installs the librarian and the binary psycopg driver.

An example configuration for the librarian is as follows (docker compose):

.. code-block:: yaml

    librarian-server:
      hostname: "librarian-docker"
      restart: always
      build:
        context: "."
        dockerfile: "Dockerfile"
      container_name: "librarian-server"
      ports:
        - 21109:21109
      stdin_open: true
      tty: true
      volumes:
        - type: "bind"
          source: "/storage/mainstore"
          target: "/storage/mainstore"
        - type: "bind"
          source: "/sneakerA"
          target: "/sneakerA"
          bind:
            propagation: "rslave"
        - type: "bind"
          source: "/users/me/site-librarian-configs/"
          target: "/librarian-configs"
      environment:
        - LIBRARIAN_CONFIG_PATH=/librarian-configs/server_config.json
        - LIBRARIAN_SERVER_DATABASE_USER=librarian
        - LIBRARIAN_SERVER_DATABASE_PASSWORD=hello-world-password
        - LIBRARIAN_SERVER_ALEMBIC_CONFIG_PATH=/librarian-configs
        - LIBRARIAN_BACKGROUND_CONFIG=/librarian-configs/background_config.json
        - LIBRARIAN_SERVER_SLACK_WEBHOOK_URL_FILE=/librarian-configs/SLACK_KEY
        - LIBRARIAN_SERVER_ENCRYPTION_KEY_FILE=/librarian-configs/FERNET_KEY
      depends_on:
        librarian-database:
          condition: service_healthy
      networks:
        - "librarian-network"

    networks:
      librarian-network:

A key thing to note here is that the storage locations have the same path inside
and outside of the container. This is because the librarian uses the file system
to store files, and so the paths must be the same.

By combining these files, alongside the correct configuration, you can deploy
with a simple ``docker compose up -d``.


Post-Setup
----------

After the initial docker deployment of the librarian, it will not work. You will
need to log into the container and run the initial database migration; this will
create the necessary tables in the database, as well as some initial rows (e.g.
the intiial admin user).

You can find the location of your running docker service with ``docker ps``
and log in with ``docker exec -it $CONTAINER_ID /bin/bash``. From here, you should
run the initial setup script:

.. code-block:: bash

    librarian-server-setup --initial-user=$INITIAL_ADMIN_USER_NAME \
                           --initial-password=$INITIAL_ADMIN_PASSWORD

Which sets up the initial administrator user and the stores in the system. To run 
a database migration, instead of the initial setup, you can run ``--migrate`` as
the argument here which will run the alembic migrations.

After this, the librarian should be up and running and you can access it at the
specified port using your `API client <Client.rst>`_.
