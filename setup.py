from distutils.core import setup
import glob

__version__ = '0.1'

setup_args = {
    'name': 'hera_librarian',
    'author': 'HERA Team',
    'license': 'BSD',
    'packages': ['hera_librarian'],
    'scripts': [
        'scripts/add_obs_librarian.py',
        # TODO: port others from PHP
    ],
    'version': __version__
}

if __name__== '__main__':
    apply(setup, (), setup_args)
