## -*- docker-image-name: "librarian-image" -*-

FROM continuumio/miniconda3:4.8.2

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# copy app
COPY . .

# update base environment
RUN conda update conda && conda update --all
RUN conda env update --file ci/librarian_server_conda_env.yml

# install
RUN pip install .[server]

# launch server
ENTRYPOINT ["./ci/start.sh"]
