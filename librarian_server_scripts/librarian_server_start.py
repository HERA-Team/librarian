#!python3

"""
Runs the librarian server. This is really just an _example_ for how to run
the server; you can run the librarian_server module with any ASGI
server framework (e.g. guivcorn if you needed more threads), and
you can even run the librarian_background module as a separate instance.
"""

from librarian_server.settings import server_settings

from librarian_server.database import session, engine
from librarian_server.orm import StoreMetadata

from librarian_server.logger import log

from librarian_background import background

from pathlib import Path

import subprocess

# Do this in if __name__ == "__main__" so we can spawn threads on MacOS...

def main():
    log.info("Librarian-server-start settings: " + str(server_settings))
    # Perform pre-startup tasks!
    log.debug("Creating the database.")
    return_value = subprocess.call(f"cd {server_settings.alembic_config_path}; {server_settings.alembic_path} upgrade head", shell=True)
    if return_value != 0:
        log.debug("Error creating or updating the database. Exiting.")
        exit(0)
    else:
        log.debug("Successfully created or updated the database.")

    log.debug("Adding store metadata to database.")

    stores_added = 0

    for store_config in server_settings.add_stores:
        if session.query(StoreMetadata).filter(StoreMetadata.name == store_config.store_name).first():
            log.debug(f"Store {store_config.store_name} already exists in database.")
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

    log.debug(f"Added {stores_added} store to the database. Committing.")

    if stores_added > 0:
        session.commit()

    # Now we can start the background process thread.
    log.info("Starting background process.")

    from multiprocessing import Process

    background_process = Process(target=background)
    background_process.start()

    # Now we can actually start the server.
    log.info("Creating uvicorn instance.")

    import uvicorn

    uvicorn.run(
        "librarian_server:app",
        port=server_settings.port,
        log_level=server_settings.log_level.lower(),
    )

    log.info("Server shut down.")

    log.info("Waiting for background process to finish.")
    background_process.terminate()
    log.info("Background process finished.")