"""
Example setup file for a project using Celery.

This can be used to distribute your tasks and worker
as a Python package, on PyPI or on your own private package index.

"""
from __future__ import absolute_import, unicode_literals

from setuptools import setup, find_packages

setup(
    name='example-tasks',
    version='1.0',
    description='Tasks for my project',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    zip_safe=False,
    install_requires=[
        'celery>=4.0',
        #  'requests',
    ],
)
