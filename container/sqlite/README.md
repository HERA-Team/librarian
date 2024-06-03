This is a very simple setup for the librarian, using a local SQLite database
as the core librarian database. Production setups may want to use the
postgres container.

You will need to run the `pre_docker_setup.sh` script to put things in the right
place before, edit server_config.json a little, and run:

- `docker compose build`
- `docker compose up`

A few things to keep in mind when deploying the librarian:

- This is inherently not secure by default; you will need to change the administrator
  password for the librarian and potentially provision further accounts.

- You will need to mark both the hostname of the docker container and of your
  local machine as available for local transfers.

- You will need to make sure that the local store locations are mounted
  transparently by the docker container (i.e. you should have /path/to/store
  the same inside and outside of the container).