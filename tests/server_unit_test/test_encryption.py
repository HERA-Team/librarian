"""
Tests for encryption technology.
"""

import os

from ..server import server_setup


def test_encrypt_decrypt_cycle(tmp_path_factory):
    setup = server_setup(tmp_path_factory, name="test_server")

    env_vars = {x: None for x in setup.env.keys()}

    for env_var in list(env_vars.keys()):
        env_vars[env_var] = os.environ.get(env_var, None)
        if setup.env[env_var] is not None:
            os.environ[env_var] = setup.env[env_var]

    from librarian_server.encryption import decrypt_string, encrypt_string

    input = "hello:world"

    encrypted = encrypt_string(input)

    assert encrypted != input

    decrypted = decrypt_string(encrypted)

    assert decrypted == input

    for env_var in list(env_vars.keys()):
        if env_vars[env_var] is None:
            del os.environ[env_var]
        else:
            os.environ[env_var] = env_vars[env_var]
