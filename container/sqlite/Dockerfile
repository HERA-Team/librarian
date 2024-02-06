FROM python:3.11

RUN pip install --no-cache-dir --upgrade git+https://github.com/simonsobs/librarian.git@librarian-v2

COPY ./server_config.json server_config.json
COPY ./background_config.json background_config.json
COPY ./librarian_env .env
COPY ./alembic.ini alembic.ini
COPY ./alembic alembic

RUN echo "Server config:"
RUN cat server_config.json

RUN echo "Background config:"
RUN cat background_config.json

RUN mkdir -p /mounts/stores/libstore
RUN mkdir -p /mounts/stores/libclone
RUN mkdir -p /volumes/database

ARG LIBRARIAN_CONFIG_PATH
ARG LIBRARIAN_SERVER_DATABASE_DRIVER
ARG LIBRARIAN_SERVER_DATABASE
ARG LIBRARIAN_SERVER_PORT

ENV LIBRARIAN_CONFIG_PATH=$LIBRARIAN_CONFIG_PATH
ENV LIBRARIAN_SERVER_DATABASE_DRIVER=$LIBRARIAN_SERVER_DATABASE_DRIVER
ENV LIBRARIAN_SERVER_DATABASE=$LIBRARIAN_SERVER_DATABASE
ENV LIBRARIAN_SERVER_PORT=$LIBRARIAN_SERVER_PORT

RUN echo $LIBRARIAN_CONFIG_PATH
RUN echo $LIBRARIAN_BACKGROUND_CONFIG
RUN echo $LIBRARIAN_SERVER_PORT

RUN librarian-server-setup

ENTRYPOINT ["librarian-server-start"]

