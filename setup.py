from setuptools import setup

__version__ = '0.1.7a0'


setup(
    name='hera_librarian',
    version=__version__,
    author='HERA Team',
    author_email='hera@lists.berkeley.edu',
    url='https://github.com/HERA-Team/librarian/',
    license='BSD',
    description='A client for the HERA Librarian data management system',
    long_description='''\
The HERA Librarian is a distributed system for managing HERA collaboration
data products. This package provides client libraries that allow you to
communicate with a Librarian server. It also includes the server code,
although those modules are not installed in a standard ``pip install``.

The Librarian client and server currently only run on Python 2.
''',

    # These are the requirements for the *client library* only -- much lighter
    # than for the server.
    install_requires=[
        'astropy >=2.0',
    ],

    packages=[
        'hera_librarian',
    ],

    scripts=[
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

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Topic :: Scientific/Engineering :: Astronomy',
    ],
)
