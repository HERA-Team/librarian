FROM python:3.11

RUN pip install --no-cache-dir --upgrade "git+https://github.com/simonsobs/librarian.git@librarian-v2"
RUN pip install --no-cache-dir --upgrade "psycopg[binary,pool]"

COPY ./server_config.json server_config.json
COPY ./background_config.json background_config.json
COPY ./alembic.ini alembic.ini
COPY ./alembic alembic
COPY ./setup_vars.sh setup_vars.sh

RUN mkdir -p /tmp/store/libstore/store
RUN mkdir -p /tmp/store/libstore/staging
RUN mkdir -p /tmp/store/libclone/store
RUN mkdir -p /tmp/store/libclone/staging
RUN mkdir -p /volumes/database

CMD ["librarian-server-start"]

