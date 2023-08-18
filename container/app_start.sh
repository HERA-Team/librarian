#!/bin/bash

# put ssh config files in place
mkdir -p ~/.ssh
cp container/ssh_config ~/.ssh/config
cp /secrets/id_rsa_pub ~/.ssh/id_rsa.pub
cp /secrets/id_rsa ~/.ssh/id_rsa

# put server config in place
cp /secrets/server-config.json /usr/src/app/server-config.json

# wait for postgres to be available
./container/wait-for-it.sh db:5432 -- alembic upgrade head
exec runserver.py
