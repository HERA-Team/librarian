from setuptools import setup

package_name = "librarian_server"
__version__ = '0.1.7a0'

setup(
    name=package_name,
    version=__version__,
        author='HERA Team',
    author_email='hera@lists.berkeley.edu',
    url='https://github.com/HERA-Team/librarian/',
    license='BSD',
    description='A server for the HERA Librarian data management system',
    long_description='''\
The HERA Librarian is a distributed system for managing HERA collaboration
data products. This package provides server libraries that allow you to
launch and run a Librarian server.

The Librarian client and server currently only run on Python 2.
''',
    install_requires=[
        'aipy',
        'alembic',
        'astropy >=2.0',
        'flask',
        'flask-sqlalchemy',
        'hera-librarian',
        'numpy',
        'psycopg2',
        'pyuvdata',
        'sqlalchemy',
    ],
    tests_require=[
        'hera-librarian',
        'pytest-datafiles',
    ]
    packages=['librarian_server'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Topic :: Scientific/Engineering :: Astronomy',
    ],
)
