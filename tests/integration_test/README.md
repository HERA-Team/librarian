Integration Testing
===================

This 'test' creates two librarian servers running on your computer and attempts to
perform numerous upload and synchronisation tasks, as well as creating databases.

Systems like the librarian and notoriously hard to test. Unit tests can only get us
so far, and in fact a full integration test like this is likely to be the most
useful in the long run.

These tests use the following libraries and features that are crucial:

- pytest (for actually running the tests)
- pytest fixtures (for setting up state, notably the server(s) themselves)
- pytest-xprocess (for running the server processes in the background).

It must be ran from the main directory in the repository with:

```
python3 -m pytest tests/integration_test
```

Note that it is not possible to garner code coverage for the librarian
server for this test, unfortunately. You must use the unit test.