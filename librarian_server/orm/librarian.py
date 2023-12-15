"""
ORM model for another librarian that we are (bi)-directionally connected
to.
"""

from .. import database as db

class Librarian(db.Model):
    """
    A librarian that we are connected to. This should be pinged every now and then
    to confirm its availability. We will then ask for a response to see if that
    librarian knows about US; they must be able to 'call us back' for
    asynchronous transfers.
    """

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    "Unique ID of this librarian (relative to us)."
    name = db.Column(db.String(256), nullable=False, unique=True)
    "The name of this librarian."
    url = db.Column(db.String(256), nullable=False)
    "The URL of this librarian."

    
