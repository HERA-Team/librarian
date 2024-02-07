"""
ORM model for a user.
"""

import argon2
from sqlalchemy.orm import Session

from ...hera_librarian.authlevel import AuthLevel
from .. import database as db


class User(db.Base):
    """
    A user in the librarian system, along with authentication functons.
    """

    __tablename__ = "users"

    username = db.Column(db.String(256), primary_key=True, unique=True)
    "The username of the user."
    auth_token = db.Column(db.String(256), nullable=False)
    "The authentication token for the user (a salted and hashed password with argon2)."
    auth_level = db.Column(db.Enum(AuthLevel), nullable=False)
    "The authorization level of the user."

    @classmethod
    def new_user(cls, username: str, password: str, auth_level: int) -> "User":
        """
        Create a new user in the database.

        Parameters
        ----------
        username : str
            The username of the new user.
        password : str
            The password of the new user.
        auth_level : int
            The authorization level of the new user.

        Returns
        -------
        User
            The new user.
        """
        # Create a new user.
        ph = argon2.PasswordHasher()

        user = cls(
            username=username,
            auth_token=ph.hash(password),
            auth_level=auth_level,
        )

        return user

    @classmethod
    def check_user(cls, username: str, password: str, session: Session) -> AuthLevel:
        """
        Check if a user exists and the password is correct.

        Parameters
        ----------
        username : str
            The username to check.
        password : str
            The password to check.
        session : Session
            The database session to use.

        Returns
        -------
        AuthLevel
            The authorization level of the user.
        """

        potential_user = session.get(User, username)

        if potential_user is not None:
            try:
                if potential_user.check_password(password):
                    return potential_user.auth_level
                else:
                    return AuthLevel.NONE
            except argon2.exceptions.VerifyMismatchError:
                return AuthLevel.NONE

        return AuthLevel.NONE

    def check_password(self, password: str) -> bool:
        """
        Check if the password is correct for this user.

        Parameters
        ----------
        password : str
            The password to check.

        Returns
        -------
        bool
            True if the password is correct.
        """
        ph = argon2.PasswordHasher()

        return ph.verify(self.auth_token, password)
