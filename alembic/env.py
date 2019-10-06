# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""This script is some boilerplate needed by Alembic to do its fancy database
migration stuff.

"""

# A hack so that we can get the librarian_server module.
import sys
sys.path.insert(0, '.')


from alembic import context

from logging.config import fileConfig
config = context.config
fileConfig(config.config_file_name)

from librarian_server import app, db
target_metadata = db.metadata


def run_migrations_offline():
    """Run migrations in 'offline' mode -- all we need is a URL.

    """
    url = app.config['SQLALCHEMY_DATABASE_URI']
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode -- using the actual Librarian database
    connection.

    """
    with db.engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
