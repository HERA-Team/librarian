## -*- docker-image-name: "librarian-image" -*-

FROM continuumio/miniconda3

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# update conda installation
RUN conda update conda \
        && conda update --all \
        && conda install \
          alembic \
          astropy \
          flask \
          flask-sqlalchemy \
          jinja2 \
          numpy \
          psycopg2 \
          pytz \
          sqlalchemy \
          tornado \
        && conda install -c conda-forge \
          aipy \
          pyuvdata

# copy app and install
COPY . .
RUN pip install .[server]

# launch server
ENTRYPOINT ["./ci/start.sh"]
