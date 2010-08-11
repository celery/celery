#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs
import platform

try:
    from setuptools import setup, find_packages, Command
    from setuptools.command.test import test as TestCommand
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages, Command
    from setuptools.command.test import test as TestCommand

import celery as distmeta


class QuickRunTests(TestCommand):
    extra_env = dict(SKIP_RLIMITS=1, QUICKTEST=1)

    def run(self, *args, **kwargs):
        for env_name, env_value in self.extra_env.items():
            os.environ[env_name] = str(env_value)
        TestCommand.run(self, *args, **kwargs)


install_requires = []

try:
    import importlib
except ImportError:
    install_requires.append("importlib")


install_requires.extend([
    "python-dateutil",
    "mailer",
    "sqlalchemy",
    "anyjson",
    "carrot>=0.10.5",
    "pyparsing",
])

py_version = sys.version_info
if sys.version_info < (2, 6):
    install_requires.append("multiprocessing==2.6.2.1")
if sys.version_info < (2, 5):
    install_requires.append("uuid")

if os.path.exists("README.rst"):
    long_description = codecs.open("README.rst", "r", "utf-8").read()
else:
    long_description = "See http://pypi.python.org/pypi/celery"

setup(
    name='celery',
    version=distmeta.__version__,
    description=distmeta.__doc__,
    author=distmeta.__author__,
    author_email=distmeta.__contact__,
    url=distmeta.__homepage__,
    platforms=["any"],
    license="BSD",
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    scripts=["bin/celeryd", "bin/celerybeat",
             "bin/camqadm", "bin/celeryd-multi",
             "bin/celeryev"],
    zip_safe=False,
    install_requires=install_requires,
    tests_require=['nose', 'nose-cover3', 'unittest2', 'simplejson'],
    cmdclass = {"quicktest": QuickRunTests},
    test_suite="nose.collector",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.4",
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
    ],
    entry_points={
        'console_scripts': [
            'celeryd = celery.bin.celeryd:main',
            'celerybeat = celery.bin.celerybeat:main',
            'camqadm = celery.bin.camqadm:main',
            'celeryev = celery.bin.celeryev:main',
            'celeryd-multi = celery.bin.celeryd_multi:main',
            ],
    },
    long_description=long_description,
)
