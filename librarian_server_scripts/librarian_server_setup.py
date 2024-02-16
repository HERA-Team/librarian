"""
Initial server setup.

Checks whether you have an empty database, unless you have the `--migrate` flag.
"""

import argparse as ap
import subprocess

from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError, InternalError, ProgrammingError

from hera_librarian.authlevel import AuthLevel
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
            exit(1)
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
        try:
            user = User.new_user(
                username=args.initial_user,
                password=args.initial_password,
                auth_level=AuthLevel.ADMIN,
            )
            session.add(user)
            session.commit()
        except IntegrityError:
            log.debug(f"User {args.initial_user} already exists in database.")
            session.rollback()

    log.debug(f"Initial user {args.initial_user} created.")

    if "postgres" not in server_settings.sqlalchemy_database_uri:
        log.debug(
            "Database is not postgres; SQLite does not play as nicely with roles. Exiting."
        )
        exit(0)

    # log.debug("Creating new database user, role, and password.")

    # with engine.begin() as conn:
    #     try:
    #         conn.execute(text("CREATE ROLE libserver"))

    #         # Granting INSERT, SELECT, UPDATE, DELETE, REFERENCES privaleges.
    #         conn.execute(
    #             text(
    #                 "GRANT INSERT,SELECT,UPDATE,DELETE,REFERENCES ON files,"
    #                 "instances,incoming_transfers,outgoing_transfers"
    #                 ",clone_transfers,remote_instances"
    #                 ",users TO libserver"
    #             )
    #         )
    #         # Grant just SELECT, REFERENCES privaleges.
    #         conn.execute(
    #             text(
    #                 "GRANT SELECT,REFERENCES ON store_metadata,librarians TO libserver"
    #             )
    #         )
    #         # Grant INSERT, SELECT, UPDATE privaleges.
    #         conn.execute(
    #             text("GRANT INSERT,SELECT,UPDATE,REFERENCES ON errors TO libserver")
    #         )
    #     except ProgrammingError:
    #         log.error("Role libserver already exists.")

    #     try:
    #         conn.execute(
    #             text(
    #                 f"CREATE USER {args.librarian_db_user} WITH PASSWORD {args.librarian_db_password}"
    #             )
    #         )
    #         conn.execute(text(f"GRANT libserver TO {args.librarian_db_user}"))
    #     except InternalError:
    #         log.error(f"User {args.librarian_db_user} already exists.")
    #         exit(1)

    # log.debug(f"Database user {args.librarian_db_user}, role, and password created.")


if __name__ == "__main__":  # pragma: no cover
    main()
