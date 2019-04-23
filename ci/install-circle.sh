set -xe

# get conda set up
apt-get update; apt-get install -y gcc g++ openssh-server
conda config --set always_yes yes --set changeps1 no
conda update -q conda
conda config --add channels conda-forge
conda info -a
conda create --name=${ENV_NAME} python=$PYTHON --quiet
conda env update -f ci/${ENV_NAME}.yml
conda init bash
source ~/.bashrc
conda activate ${ENV_NAME}
conda list -n ${ENV_NAME}

# install other dependencies with pip
pip install pytest-datafiles pytest-cov

# set up librarian database
cd server
alembic upgrade head

# set up ssh server
/etc/init.d/ssh start

# add localhost to known_hosts
ssh-keyscan -H localhost >> ~/.ssh/known_hosts
