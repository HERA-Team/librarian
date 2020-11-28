#!/bin/bash

# wait for postgres to be available
./ci/wait-for-it.sh db:5432 -- alembic upgrade head
runserver.py
