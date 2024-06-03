This is a more complex librarian setup, and so requires a little more thought when setting
it up.

A few things to keep in mind when deploying the librarian:

- This is inherently not secure by default; you will need to change the administrator
  password for the librarian and potentially provision further accounts.

- You will need to mark both the hostname of the docker container and of your
  local machine as available for local transfers.

- You will need to make sure that the local store locations are mounted
  transparently by the docker container (i.e. you should have /path/to/store
  the same inside and outside of the container).

Specifically for this librarian, as we have the database running in a different Docker
container, we'll need to make sure that this is up and running before running anything
like a database migration.

So, you will need to:

- `bash pre_docker_setup.sh`
- `docker compose build`
- `docker compose up` (this will cause errors!)
- `docker ps` -> grab the ID of the running librarian server instance
- `docker exec -ti {ID} bash` -> into the container
- `source setup_vars.sh`
- `librarian-server-setup`
- `exit`
- Restart the docker container.
- Your application should now be happily running.