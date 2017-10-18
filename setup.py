from distutils.core import setup

__version__ = '0.1.2.99'

setup_args = {
    'name': 'hera_librarian',
    'author': 'HERA Team',
    'license': 'BSD',
    'packages': ['hera_librarian'],
    'scripts': [
        'scripts/add_librarian_file_event.py',
        'scripts/add_obs_librarian.py',
        'scripts/launch_librarian_copy.py',
        'scripts/librarian_assign_sessions.py',
        'scripts/librarian_delete_files.py',
        'scripts/librarian_initiate_offload.py',
        'scripts/librarian_locate_file.py',
        'scripts/librarian_offload_helper.py',
        'scripts/librarian_set_file_deletion_policy.py',
        'scripts/librarian_stage_files.py',
        'scripts/librarian_stream_file_or_directory.sh',
        'scripts/upload_to_librarian.py',
    ],
    'version': __version__
}

if __name__ == '__main__':
    apply(setup, (), setup_args)
