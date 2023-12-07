"""
ORM for the 'instances' table, describing locations of files on stores.

Note that this does not include instances on other librarians.

# TODO: Track those other instances in a separate table.
"""

from .. import database as db
from ..deletion import DeletionPolicy

from datetime import datetime
from pathlib import Path


class Instance(db.Base):
    """
    Represents an instance of a file on a Store. Files are unique, Instances are not;
    there may be many copies of a single 'File' on several stores.
    """

    __tablename__ = "instances"

    # NOTE: SQLite does not allow autoincrement PKs that are BigIntegers.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this instance."
    path = db.Column(db.String(256), nullable=False)
    "Full path on the store."
    file_name = db.Column(db.String(256), db.ForeignKey("files.name"), nullable=False)
    "Name of the file this instance references."
    file = db.relationship("File", back_populates="instances", primaryjoin="Instance.file_name == File.name")
    "The file that object is an instance of."
    store_id = db.Column(db.Integer, db.ForeignKey("store_metadata.id"), nullable=False)
    "ID of the store this instance is on."
    store = db.relationship("StoreMetadata", primaryjoin="Instance.store_id == StoreMetadata.id")
    "The store that this object is on."
    deletion_policy = db.Column(db.Enum(DeletionPolicy), nullable=False)
    "Whether or not this file can be deleted from the store."
    created_time = db.Column(db.DateTime, nullable=False)
    "The time at which this file was placed on the store."

    @classmethod
    def new_instance(
            self, path: Path, file: "File", store: "StoreMetadata", deletion_policy: DeletionPolicy
        ) -> "Instance":
            """
            Create a new instance object.

            Parameters
            ----------
            path : Path
                The path of the instance.
            file : File
                The file that this instance is of.
            store : StoreMetadata
                The store that this instance is on.
            deletion_policy : DeletionPolicy
                The deletion policy for this instance.

            Returns
            -------
            Instance
                The new instance.
            """

            return Instance(
                path=str(path),
                file=file,
                store=store,
                deletion_policy=deletion_policy,
                created_time=datetime.utcnow(),
            )
