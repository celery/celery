#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

try:
    from setuptools import setup
    from setuptools.command.install import install
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup  # noqa
    from setuptools.command.install import install  # noqa

import os
import sys

sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), os.pardir))
import suite  # noqa


class no_install(install):

    def run(self, *args, **kwargs):
        import sys
        sys.stderr.write("""
-----------------------------------------------------
The Celery functional test suite cannot be installed
-----------------------------------------------------


But you can execute the tests by running the command:

    $ python setup.py test


""")


setup(
    name='celery-funtests',
    version='DEV',
    description='Functional test suite for Celery',
    author='Ask Solem',
    author_email='ask@celeryproject.org',
    url='https://github.com/celery/celery',
    keywords='celery integration tests',
    license='BSD',
    platforms=['any'],
    packages=[],
    data_files=[],
    zip_safe=False,
    cmdclass={'install': no_install},
    test_suite='nose.collector',
    tests_require=[
        'unittest2>=0.4.0',
        'simplejson',
        'nose',
        'redis',
        'pymongo',
    ],
    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    long_description=__doc__,
)
