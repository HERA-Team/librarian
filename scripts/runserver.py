"""
Runs the librarian server.
"""

from librarian_server.settings import server_settings

from librarian_server.database import session, engine
from librarian_server.orm import StoreMetadata

from librarian_server.logger import log

# Perform pre-startup tasks!
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
        store_data={**store_config.store_data, "name": store_config.store_name},
        transfer_manager_data=store_config.transfer_manager_data,
    )

    session.add(store)

    stores_added += 1

log.debug(f"Added {stores_added} store to the database. Committing.")

if stores_added > 0:
    session.commit()

# Now we can actually start the server.
log.info("Creating uvicorn instance.")

import uvicorn

uvicorn.run(
    "librarian_server:app",
    port=server_settings.port,
    log_level=server_settings.log_level.lower(),
)