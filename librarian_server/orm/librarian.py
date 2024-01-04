"""
ORM model for another librarian that we are (bi)-directionally connected
to.
"""

from datetime import datetime
from .. import database as db

from hera_librarian import LibrarianClient, LibrarianHTTPError
from pydantic import ValidationError

class Librarian(db.Base):
    """
    A librarian that we are connected to. This should be pinged every now and then
    to confirm its availability. We will then ask for a response to see if that
    librarian knows about US; they must be able to 'call us back' for
    asynchronous transfers.
    """

    __tablename__ = "librarians"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    "Unique ID of this librarian (relative to us)."
    name = db.Column(db.String(256), nullable=False, unique=True)
    "The name of this librarian."
    url = db.Column(db.String(256), nullable=False)
    "The URL of this librarian."
    port = db.Column(db.Integer, nullable=False)
    "The port of this librarian."
    authenticator = db.Column(db.String(256), nullable=False)
    "The authenticator so we can connect this librarian."
    # TODO: THIS IS A MASSIVE HOLE IN SECURITY THAT ABSOLUTELY MUST BE FIXED
    # URGENT: FIX THIS.

    last_seen = db.Column(db.DateTime, nullable=False)
    "The last time we connected to and verified this librarian exists."
    last_heard = db.Column(db.DateTime, nullable=False)
    "The last time we heard from this librarian (the last time it connected to us)."


    @classmethod
    def new_librarian(self, name: str, url: str, port: int) -> "Librarian":
        """
        Create a new librarian object.

        Parameters
        ----------
        name : str
            The name of this librarian.
        url : str
            The URL of this librarian.
        port : int
            The port of this librarian.

        Returns
        -------
        Librarian
            The new librarian.
        """

        librarian = Librarian(
            name=name,
            url=url,
            port=port,
            last_seen=datetime.utcnow(),
            last_heard=datetime.utcnow()
        )

        # Before returning it, we should ping it to confirm it exists.

        client = librarian.client()

        try:
            client.ping()
        except LibrarianHTTPError:
            raise ValueError(
                "Librarian does not exist or is unreachable."
            )
        except ValidationError:
            raise ValueError(
                "Librarian does not conform to specification and "
                "is returning an invalid response to ping."
            )
        
        librarian.last_seen = datetime.utcnow()
        
        return librarian


    def client(self) -> LibrarianClient:
        """
        Create a client for this librarian.

        Returns
        -------
        LibrarianClient
            The client.
        """

        return LibrarianClient(
            conn_name=self.name,
            conn_config={
                "url": f"{self.url}:{self.port}",
                "authenticator": self.authenticator
            })



    

    
