#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
import sys

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

import crunchy

install_requires = ["carrot", "django"]
py_version_info = sys.version_info
py_major_version = py_version_info[0]
py_minor_version = py_version_info[1]

if (py_major_version == 2 and py_minor_version <=5) or py_major_version < 2:
    install_requires.append("multiprocessing")    

setup(
    name='crunchy',
    version=crunchy.__version__,
    description=crunchy.__doc__,
    author=crunchy.__author__,
    author_email=crunchy.__contact__,
    url=crunchy.__homepage__,
    platforms=["any"],
    packages=find_packages(exclude=['ez_setup']),
    scripts=["crunchy/bin/crunchd"],
    install_requires=[
        'simplejson',
        'carrot',
        'django',
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Django",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    long_description=codecs.open('README.rst', "r", "utf-8").read(),
)
