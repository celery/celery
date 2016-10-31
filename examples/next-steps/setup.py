"""
Example setup file for a project using Celery.

This can be used to distribute your tasks and worker
as a Python package, on PyPI or on your own private package index.

"""
from __future__ import absolute_import, unicode_literals
from setuptools import setup, find_packages

setup(
    name='example-tasks',
    url='http://github.com/example/celery-tasks',
    author='Ola A. Normann',
    author_email='author@example.com',
    keywords='our celery integration',
    version='1.0',
    description='Tasks for my project',
    long_description=__doc__,
    license='BSD',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    test_suite='nose.collector',
    zip_safe=False,
    install_requires=[
        'celery>=4.0',
        #  'requests',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Operating System :: OS Independent',
    ],
)
