#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import codecs

try:
    from setuptools import setup, find_packages
    from setuptools.command.test import test
except ImportError:
    raise
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages           # noqa
    from setuptools.command.test import test              # noqa

if os.path.exists("README.rst"):
    long_description = codecs.open("README.rst", "r", "utf-8").read()
else:
    long_description = "See http://pypi.python.org/pypi/celery"

setup(
    name="celery-with-redis",
    version='2.4.0',
    description="Bundle that installs the dependencies for Celery and Redis",
    author="Celery Project",
    author_email="bundles@celeryproject.org",
    url="http://celeryproject.org",
    platforms=["any"],
    license="BSD",
    packages=[],
    zip_safe=False,
    install_requires=[
        "celery>=2.4.0,<3.0.0",
        "redis>=2.4.4",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: BSD License",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
    ],
    long_description=long_description)
