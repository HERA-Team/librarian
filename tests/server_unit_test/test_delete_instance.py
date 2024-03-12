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
from hera_librarian.models.admin import (
    AdminDeleteInstanceRequest,
    AdminDeleteInstanceResponse,
)


def test_delete_local_instance(test_server, test_orm, test_client):

    request = AdminDeleteInstanceRequest(instance_id=293472397249)
    response = test_client.post_with_auth(
        "api/v2/admin/instance" "/delete_local", content=request.model_dump_json()
    )

    assert response.status_code == 400

    session = test_server[1]()

    store = session.query(test_orm.StoreMetadata).first()

    data = random.randbytes(1024)

    file_to_delete = test_orm.File.new_file(
        filename="example_file_test_delete_local_instance.txt",
        size=len(data),
        checksum=hashlib.md5(data).hexdigest(),
        uploader="test",
        source="test",
    )

    # Create the file in the store
    path = store.store_manager._resolved_path_store(Path(file_to_delete.name))

    with open(path, "wb") as handle:
        handle.write(data)

    instance = test_orm.Instance.new_instance(
        path=path,
        file=file_to_delete,
        store=store,
        deletion_policy="ALLOWED",
    )

    session.add_all([file_to_delete, instance])
    session.commit()

    instance_id = instance.id

    session.close()
    request = AdminDeleteInstanceRequest(instance_id=instance_id, delete_file=True)

    response = test_client.post_with_auth(
        "api/v2/admin/instance/delete_local", content=request.model_dump_json()
    )

    assert response.status_code == 200
    assert not Path.exists(path)
