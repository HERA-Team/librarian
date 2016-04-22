from distutils.core import setup

__version__ = '0.1'

setup_args = {
    'name': 'hera_librarian',
    'author': 'HERA Team',
    'license': 'BSD',
    'packages': ['hera_librarian'],
    'scripts': [
        'scripts/add_obs_librarian.py',
        'scripts/upload_to_librarian.py',
        'scripts/launch_librarian_copy.py',
    ],
    'version': __version__
}

if __name__== '__main__':
    apply(setup, (), setup_args)
