# -*- coding: utf-8 -*-
# Copyright (c) 2019 The HERA Collaboration
# Licensed under the 2-clause BSD License

import os
import re
import codecs
from setuptools import setup, find_packages

package_name = "hera_librarian"

packages = find_packages(exclude=["*.tests"])


setup(
    name=package_name,
    version="1.1.1",
    author="HERA Team",
    author_email="hera@lists.berkeley.edu",
    url="https://github.com/HERA-Team/librarian/",
    license="BSD",
    description="A client for the HERA Librarian data management system",
    long_description="""\
The HERA Librarian is a distributed system for managing HERA collaboration
data products. This package provides client libraries that allow you to
communicate with a Librarian server. It also includes the server code,
although those modules are not installed in a standard ``pip install``.
""",
    install_requires=["astropy >=2.0"],
    tests_require=["pytest", "pytest-datafiles", "pytest-console-scripts"],
    packages=packages,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Topic :: Scientific/Engineering :: Astronomy",
    ],
    extras_require={
        "server": [
            "aipy",
            "alembic",
            "astropy >=2.0",
            "flask",
            "flask-sqlalchemy",
            "hera-librarian",
            "numpy",
            "psycopg2-binary",
            "pytz",
            "pyuvdata",
            "sqlalchemy>=1.4.0",
        ]
    },
    scripts=[
        "scripts/librarian_stream_file_or_directory.sh",
        "scripts/runserver.py",
    ],
    entry_points={"console_scripts": ["librarian=hera_librarian.cli:main"]},
    use_scm_version=True,
    setup_requires=["setuptools_scm", "setuptools_scm_git_archive"],
    include_package_data=True,
    zip_safe=False,
)
