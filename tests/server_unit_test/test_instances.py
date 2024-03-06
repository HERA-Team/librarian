"""
Tests the /users endpoints.
"""

import datetime
import hashlib
import json
import os
import random
import shutil
import sys
from pathlib import Path
from subprocess import run

import pytest

from hera_librarian.authlevel import AuthLevel
from hera_librarian.models.instances import (
    InstanceAdministrationDeleteRequest,
    InstanceAdministrationChangeResponse,
)


def test_delete_local_instance(test_server, test_orm, test_client):

    request = InstanceAdministrationDeleteRequest(instance_id=293472397249)
    response = test_client.post_with_auth(
        "api/v2/instances/delete_local_instance", content=request.model_dump_json()
    )

    assert response.status_code == 400

    session = test_server[1]()

    store = session.query(test_orm.StoreMetadata).first()

    data = random.randbytes(1024)

    file = test_orm.File.new_file(
        filename="example_file.txt",
        size=len(data),
        checksum=hashlib.md5(data).hexdigest(),
        uploader="test",
        source="test",
    )

    # Create the file in the store
    path = store.store_manager._resolved_path_store(Path(file.name))

    with open(path, "wb") as handle:
        handle.write(data)

    instance = test_orm.Instance.new_instance(
        path=path,
        file=file,
        store=store,
        deletion_policy="ALLOWED",
    )

    session.add_all([file, instance])
    session.commit()

    instance_id = instance.id

    session.close()
    request = InstanceAdministrationDeleteRequest(instance_id=instance_id)

    response = test_client.post_with_auth(
        "api/v2/instances/delete_local_instance", content=request.model_dump_json()
    )
    assert response.status_code == 200
