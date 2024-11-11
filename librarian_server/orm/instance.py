"""
ORM for the 'instances' table, describing locations of files on stores.
Also includes the ORM for the 'remote_instances' table, describing
what files have instances on remote librarians that we are aware about.
"""

from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from hera_librarian.deletion import DeletionPolicy

from .. import database as db
from ..settings import server_settings


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
    file = db.relationship(
        "File",
        back_populates="instances",
        primaryjoin="Instance.file_name == File.name",
    )
    "The file that object is an instance of."
    store_id = db.Column(db.Integer, db.ForeignKey("store_metadata.id"), nullable=False)
    "ID of the store this instance is on."
    store = db.relationship(
        "StoreMetadata", primaryjoin="Instance.store_id == StoreMetadata.id"
    )
    "The store that this object is on."
    deletion_policy = db.Column(db.Enum(DeletionPolicy), nullable=False)
    "Whether or not this file can be deleted from the store."
    created_time = db.Column(db.DateTime, nullable=False)
    "The time at which this file was placed on the store."
    available = db.Column(db.Boolean, nullable=False)
    "Whether or not this file is available on our librarian."

    @classmethod
    def new_instance(
        self,
        path: Path,
        file: "File",
        store: "StoreMetadata",
        deletion_policy: DeletionPolicy,
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
            created_time=datetime.now(timezone.utc),
            available=True,
        )

    def delete(
        self,
        session: Session,
        commit: bool = True,
        force: bool = False,
    ):
        """
        Delete this instance.

        Parameters
        ----------
        session : Session
            The session to use for the deletion.
        commit : bool
            Whether or not to commit the deletion.
        force : bool
            Whether or not to force the deletion (i.e. ignore DeletionPolicy)
        """

        if self.deletion_policy == DeletionPolicy.ALLOWED or force:
            self.store.store_manager.delete(Path(self.path))

        session.delete(self)

        if commit:
            session.commit()

        return


class RemoteInstance(db.Base):
    """
    Remote instances of files, i.e. instances of files on remote librarians
    that we know about.
    """

    __tablename__ = "remote_instances"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this instance."
    file_name = db.Column(db.String(256), db.ForeignKey("files.name"), nullable=False)
    "Name of the file this instance references."
    file = db.relationship(
        "File",
        back_populates="remote_instances",
        primaryjoin="RemoteInstance.file_name == File.name",
    )
    "The file that object is an instance of."
    store_id = db.Column(db.Integer, nullable=False)
    "The store ID on the remote librarian."
    librarian_id = db.Column(db.Integer, db.ForeignKey("librarians.id"), nullable=False)
    "ID of the librarian this instance is on."
    librarian = db.relationship(
        "Librarian", primaryjoin="RemoteInstance.librarian_id == Librarian.id"
    )
    "The librarian that this object is on."
    copy_time = db.Column(db.DateTime, nullable=False)
    "The time at which this file was confirmed as being fully copied to the remote librarian."
    sender = db.Column(db.String(256), nullable=False)
    "The name of the librarian that sent this file to the remote librarian."

    @classmethod
    def new_instance(
        self, file: "File", store_id: int, librarian: "Librarian"
    ) -> "RemoteInstance":
        """
        Create a new remote instance object for a clone that was
        created by us.

        Parameters
        ----------
        file : File
            The file that this instance is of.
        store_id : int
            The store ID on the remote librarian.
        librarian : Librarian
            The librarian that this instance is on.

        Returns
        -------
        RemoteInstance
            The new instance.
        """

        return RemoteInstance(
            file=file,
            store_id=store_id,
            librarian_id=librarian.id,
            librarian=librarian,
            copy_time=datetime.now(timezone.utc),
            sender=server_settings.name,
        )
