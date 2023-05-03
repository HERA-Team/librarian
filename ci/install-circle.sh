set -xe

# get conda set up
apt-get update; apt-get install -y gcc g++ openssh-server rsync
mamba config --set always_yes yes --set changeps1 no
mamba install setuptools pip
mamba update -q conda
mamba config --add channels conda-forge
mamba info -a
mamba create --name=${ENV_NAME} python=$PYTHON --quiet
mamba env update -f ci/${ENV_NAME}.yml
mamba init bash
source ~/.bashrc
mamba activate ${ENV_NAME}
mamba list -n ${ENV_NAME}

# install other dependencies with pip
pip install pytest-datafiles pytest-cov pytest-console-scripts

# set up librarian database
alembic upgrade head

# set up ssh server
/etc/init.d/ssh start

# add localhost to known_hosts
ssh-keyscan -H localhost >> ~/.ssh/known_hosts

# generate and add key to authorized_keys
# id_rsa already exists, so we make an ecdsa key
ssh-keygen -t ecdsa -f ~/.ssh/id_ecdsa -N ''
cat ~/.ssh/id_ecdsa.pub >> ~/.ssh/authorized_keys
