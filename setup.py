import os
import re
import codecs
from setuptools import setup, find_packages

package_name = "hera_librarian"

# get the version from __init__.py
here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

packages = find_packages(exclude=["*.tests"])

setup(
    name=package_name,
    version=find_version(package_name, "__init__.py"),
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
    install_requires=[
        'astropy >=2.0',
    ],
    tests_require=[
        'pytest',
        'pytest-datafiles',
        'pytest-console-scripts',
    ],
    packages=packages,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Topic :: Scientific/Engineering :: Astronomy',
    ],
    extras_require={
        'server': [
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
    },
    scripts=[
        "scripts/librarian_stream_file_or_directory.sh",
    ],
    entry_points={
        "console_scripts": [
            "librarian=hera_librarian.cli:main",
        ],
    },
)
