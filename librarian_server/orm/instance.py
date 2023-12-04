"""
ORM for the 'instances' table, describing locations of files on stores.

Note that this does not include instances on other librarians.

# TODO: Track those other instances in a separate table.
"""

from .. import db
from ..deletion import DeletionPolicy

from datetime import datetime


class Instance(db.Model):
    """
    Represents an instance of a file on a Store. Files are unique, Instances are not;
    there may be many copies of a single 'File' on several stores.
    """

    __tablename__ = "instances"

    # NOTE: SQLite does not allow autoincrement PKs that are BigIntegers.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this instance."
    file = db.relationship("File", back_populates="instances", nullable=False)
    "The file that object is an instance of."
    store = db.relationship("StoreMetadata", nullable=False)
    "The store that this object is on."
    deletion_policy = db.Column(db.Enum(DeletionPolicy), nullable=False)
    "Whether or not this file can be deleted from the store."
    created_time = db.Column(db.DateTime, nullable=False)

    @classmethod
    def new_instance(
        self, file: "File", store: "StoreMetadata", deletion_policy: DeletionPolicy
    ) -> "Instance":
        """
        Create a new instance object.

        Parameters
        ----------
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
            file=file,
            store=store,
            deletion_policy=deletion_policy,
            created_time=datetime.datetime.utcnow(),
        )
