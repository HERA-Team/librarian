"""
Host for store metadata and related tasks.

Includes the StoreMetadata class, which is a database model.
"""


from . import db, app, logger

from hera_librarian.stores import Stores, CoreStore

from .webutil import ServerError
from .file import DeletionPolicy, File, FileInstance
from .dbutil import SQLAlchemyError

from enum import Enum


class MetaMode(Enum):
    """
    Metadata inference mode.
    """

    INFER
    DIRECT


class StoreMetadata(db.Model):
    """
    A store is an abstracted concept of a storage location for files.

    It is made up of two key components: a staging area, and a store area.

    The staging area is where files are initially placed when they are uploaded.
    This is done to ensure that the file is fully uploaded before it is
    committed to the store area.

    Stores are defined in hera_librarian.stores.*, and are of many types, taking
    values in the enum defined therein.
    """

    # The store represented by this metadata.
    store: CoreStore

    __tablename__ = "store_metadata"

    # Unique ID of this store.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # The name of this store (as defined in the parameter file).
    name = db.Column(db.String(255), nullable=False, unique=True)
    # The type of this store. Indexes into hera_librarain.stores.Stores.
    store_type = db.Column(db.Integer, nullable=False)
    # The data required for this store.
    store_data = db.Column(db.PickleType)
    # The instances of files that are stored on this store.
    instances = db.relationship("FileInstance", back_populates="store")

    def __init__(self, name: str, store_type: int, store_data: dict):
        super().__init__(self)

        self.name = name
        self.store_type = store_type
        self.store_data = store_data

        self.store = Stores[store_type].from_dict(store_data)

    def process_staged_file(
        self,
        staged_path: Path,
        store_path: Path,
        meta_mode: MetaMode,
        deletion_policy: DeletionPolicy,
        source_name: Optional[str] = None,
        null_obsid: bool = False,
    ):
        """
        Process a staged file, moving it to the store area and creating a
        FileInstance for it.
        """

        destination_directory = store_path.parent
        destination_name = store_path.name

        if null_obsid and meta_mode != MetaModes.INFER:
            raise ServerError(
                "Internal error: null_obsid only valid when meta_mode is INFER."
            )

        # Check if we already have the intended instance. We can then just delete
        # the staged file...

        instance = FileInstance.query.get(
            (self.id, destination_directory, destination_name)
        )

        if instance is not None:
            # TODO: Is this _actually_ something that we want to do? Is this a case
            #       that we need to guard against?
            self.store.unstage(staged_path)
            return

        # ...otherwise, we need to move the staged file to the store area.

        # We need to now get the metadata for the file. We can either already have
        # been given it, or we need to infer it from the file instance (only for
        # certian kinds of files).

        if meta_mode == MetaMode.DIRECT:
            # In theory, the File should already exist in the database with all
            # the records we need.

            # Files have a unique file name.
            file_metadata = File.query.get(file_name)

            if file is None:
                # Delete the file, we have a problem!
                this.store.unstage(staged_path)

                raise ServerError(
                    f"Cannot complete upload of {self.name} to {self.store.name} with "
                    f"path {store_path}: proper metadata were not uploaded in initiate_upload "
                    "call."
                )

            # Now validate the file upload. Old code checked the size and the md5, but
            # that's redundant...

            # TODO: Actually check the md5sum.
            staged_md5: str = ""

            if not file_metadata.md5 == staged_md5:
                raise ServerError(
                    f"Cannot complete upload of {self.name} to {self.store.name} with "
                    f"path {store_path}: md5sum of staged file does not match that of "
                    f"file metadata ({staged_md5}/{file_metadata.md5})."
                )
        elif meta_mode == MetaMode.INFER:
            # We need to infer the metadata from the file instance.
            # This should be avoided, because we're unable to verify that the
            # file upload succeeded (e.g. we have no md5sum to check).

            if source_name is None:
                raise ServerError(
                    f"Cannot complete upload of {self.name} to {self.store.name} with "
                    f"path {store_path}: source_name must be provided when meta_mode is "
                    "INFER."
                )

            # This creates the File instance in the database, too.
            file_metadata = File.get_inferring_info(
                store=self.store,
                store_path=store_path,
                source_name=source_name,
                null_obsid=null_obsid,
            )
        else:
            raise ServerError(f"Invalid meta_mode value {meta_mode}.")

        # Now we can commit the file to the store, and create the FileInstace
        # in the instance database.

        # TODO: Permissions mode changes (from app.config).

        self.store.commit(staged_path, store_path)

        file_instance = FileInstance(
            store=self,
            parent_dirs=destination_directory,
            file_name=destination_name,
            deletion_policy=deletion_policy,
        )

        db.session.add(file_instance)
        db.session.add(file_metadata.make_instance_creation_event(file_instance, self))

        try:
            db.session.commit()
        except SQLAlchemyError as e:
            db.session.rollback()
            raise ServerError(
                f"Failed to commit file instance {file_instance} to database: {e}"
            )

        return file_instance

    @classmethod
    def from_name(cls, name):
        stores = cls.query.filter_by(name=name).all()

        if len(stores) == 0:
            raise ServerError(f"Store {name} does not exist")
        elif len(stores) > 1:
            raise ServerError(f"Multiple stores with name {name} exist")

        return stores[0]
