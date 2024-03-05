"""
Functions for encrypting and decrypting data.
"""

from cryptography.fernet import Fernet

from .settings import server_settings


def encrypt_string(input: str) -> str:
    """
    Encrypt the given string.
    """
    key = server_settings.encryption_key

    if key is None:
        raise ValueError("No encryption key is set!")

    f = Fernet(key=key)
    return f.encrypt(input.encode()).decode()


def decrypt_string(input: str) -> str:
    """
    Decrypt the given string.
    """
    key = server_settings.encryption_key

    if key is None:
        raise ValueError("No encryption key is set!")

    f = Fernet(key=key)
    return f.decrypt(input.encode()).decode()
