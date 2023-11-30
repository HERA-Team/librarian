"""
Contains API endpoints for uploading data to the Librarian and its
stores.
"""

from .. import app, db
from ..webutil import ServerError, json_api, required_arg, optional_arg
from ..storemetadata import StoreMetadata, MetaMode
from ..file import DeletionPolicy

from pathlib import Path
from typing import Optional

@app.route('/api/v2/uploads/stores', methods=["POST", "GET"])
@json_api
def probe_stores(args, source_name=None):
    """
    Probes the stores for their metadata and returns it.
    """

    return {
        "stores": [store.to_dict() for store in StoreMetadata.query.all()]
    }

@app.route('/api/v2/upload/stage', methods=["POST", "GET"])
@json_api
def stage(args, source_name=None):
    """
    Initiates an upload to a store.
    
    Stages a file, and returns information about the transfer
    providers that can be used by the client to upload the file.
    """

    # Figure out which store to use.
    upload_size: int = required_arg(args, int, "upload_size")

    if upload_size < 0:
        raise ServerError("Upload size must be positive.")
    
    # TODO: Original code had known_staging_store stuff here.

    use_store: Optional[StoreMetadata] = None

    for store in StoreMetadata.query.all():
        if not store.store_manager.available:
            continue

        if store.store_manager.free_space > upload_size:
            use_store = store
            break

    if use_store is None:
        raise ServerError("No stores available.")
    
    # Now generate the response; tell client to use this store.

    response = {}

    response["available_bytes_on_store"] = use_store.store_manager.free_space
    response["store_name"] = use_store.name
    response["staging_location"] = use_store.store_manager.stage(upload_size)
    response["transfer_providers"] = use_store.transfer_providers

    # TODO: Original code here had "create records" stuff.

    return response

            
@app.route("/api/v2/upload/commit", methods=["POST", "GET"])
@json_api
def commit(args, source_name=None):
    """
    Commits a file to a store, called once it has been uploaded.
    """

    store_name: str = required_arg(args, str, "store_name")

    staging_location: Path = required_arg(args, Path, "staging_location")
    destination_locaiton: Path = required_arg(args, Path, "destination_location")

    meta_mode = MetaMode.from_str(required_arg(args, str, "meta_mode"))
    deletion_policy = DeletionPolicy.parse_safe(required_arg(args, str, "deletion_policy", "disallowed"))
    source_name = required_arg(args, str, "source_name")

    null_obsid = optional_arg(args, int, "null_obsid", False)

    store: StoreMetadata = StoreMetadata.from_name(store_name)

    store.process_staged_file(
        staged_path=staging_location,
        store_path=destination_locaiton,
        meta_mode=meta_mode,
        deletion_policy=deletion_policy,
        source_name=source_name,
        null_obsid=null_obsid
    )

    # Now that the file has been processed, we can unstage the file.
    store.store_manager.unstage(staging_location)

    return {"success": True}


