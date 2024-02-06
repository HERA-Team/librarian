"""
Initial server setup.

Checks whether you have an empty database, unless you have the `--migrate` flag.
"""

import argparse as ap
import subprocess

from sqlalchemy import inspect

from librarian_server.database import engine, get_session
from librarian_server.logger import log
from librarian_server.orm import StoreMetadata
from librarian_server.settings import server_settings

parser = ap.ArgumentParser(
    description=(
        "Librarian server setup. Used to migrate databases, and create "
        "initial user accounts. After running this, you absolutely should "
        "have read the documentation and have changed the default password, "
        "as well as created a new user account. You should also change the "
        "database user and password to whatever was set here."
    )
)

parser.add_argument(
    "--migrate",
    action="store_true",
    help="Migrate the database, even if it's not empty. If we are migrating, we will not add the initial user.",
)

parser.add_argument(
    "--initial-user",
    type=str,
    help="Create an initial user with the given username.",
    default="admin",
)

parser.add_argument(
    "--initial-password",
    type=str,
    help="Set the initial user's password. Probably change this.",
    default="password",
)

parser.add_argument(
    "--librarian-db-user",
    type=str,
    help="Set the database user for the librarian database.",
    default="librarian",
)

parser.add_argument(
    "--librarian-db-password",
    type=str,
    help="Set the database password for the librarian database.",
    default="password",
)

args = parser.parse_args()


def main():
    log.info("Librarian-server-setup settings: " + str(server_settings))

    if (not inspect(engine).has_table("store_metadata")) or args.migrate:
        log.debug("Creating the database.")
        return_value = subprocess.call(
            f"cd {server_settings.alembic_config_path}; {server_settings.alembic_path} upgrade head",
            shell=True,
        )
        if return_value != 0:
            log.debug("Error creating or updating the database. Exiting.")
            exit(0)
        else:
            log.debug("Successfully created or updated the database.")

    log.debug("Adding any new store metadata to database.")

    stores_added = 0
    already_present = 0
    migrated = 0
    total = 0

    with get_session() as session:
        for store_config in server_settings.add_stores:
            total += 1

            current_store = (
                session.query(StoreMetadata)
                .filter(StoreMetadata.name == store_config.store_name)
                .first()
            )

            if current_store is not None:
                log.debug(
                    f"Store {store_config.store_name} already exists in database."
                )
                already_present += 1

                if args.migrate:
                    log.debug(f"Migrating store {store_config.store_name}.")
                    current_store.store_type = store_config.store_type
                    current_store.ingestable = store_config.ingestable
                    current_store.store_data = {
                        **store_config.store_data,
                        "name": store_config.store_name,
                    }
                    current_store.transfer_manager_data = (
                        store_config.transfer_manager_data
                    )
                    migrated += 1

                continue

            log.debug(f"Adding store {store_config.store_name} to database.")

            store = StoreMetadata(
                name=store_config.store_name,
                store_type=store_config.store_type,
                ingestable=store_config.ingestable,
                store_data={**store_config.store_data, "name": store_config.store_name},
                transfer_manager_data=store_config.transfer_manager_data,
            )

            session.add(store)
            stores_added += 1

        session.commit()

    log.debug(
        f"Added {stores_added} store to the database. {already_present} already "
        f"present out of {total}, and {migrated} were migrated."
    )

    if args.migrate:
        log.debug("Database migration complete. Exiting.")
        exit(0)

    log.debug("Creating initial user.")

    from librarian_server.orm import User

    with get_session() as session:
        user = User(username=args.initial_user, password=args.initial_password)
        session.add(user)
        session.commit()

    log.debug(f"Initial user {args.initial_user} created.")

    if "postgres" not in server_settings.database_url:
        log.debug(
            "Database is not postgres; SQLite does not play as nicely with roles. Exiting."
        )
        exit(0)

    log.debug("Creating new database user, role, and password.")

    with engine.begin() as conn:
        conn.execute("CREATE ROLE libserver")
        conn.execute(
            "GRANT INSERT AND SELECT AND UPDATE AND DELETE ON files "
            "AND instances AND incoming_transfers AND outgoing_transfers "
            "AND clone_transfers AND librarians AND remote_instances "
            "AND errors TO libserver"
        )
        conn.execute(
            f"CREATE USER {args.librarian_db_user} WITH PASSWORD '{args.librarian_db_password}'"
        )
        conn.execute(f"GRANT libserver TO {args.librarian_db_user}")

    log.debug(f"Database user {args.librarian_db_user}, role, and password created.")
