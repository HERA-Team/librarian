set -xe

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
