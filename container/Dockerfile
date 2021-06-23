## -*- docker-image-name: "librarian-app" -*-

# define base image that has librarian installed
FROM continuumio/miniconda3:4.8.2 AS librarian-base

# copy setup file
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY ci/librarian_server_conda_env.yml ci/librarian_server_conda_env.yml

# update base environment
RUN conda update conda && conda update --all
RUN conda env update --file ci/librarian_server_conda_env.yml

# copy app code
COPY . .

# install
RUN pip install .


# define image for running librarian app
FROM librarian-base AS librarian-app

# launch server
ENTRYPOINT ["./container/app_start.sh"]


# define image for running store
FROM librarian-base AS librarian-store

# install openssh-server
RUN apt-get update && apt-get install -y openssh-server rsync
RUN mkdir -p /run/sshd
EXPOSE 22
ENTRYPOINT ["./container/store_start.sh"]
