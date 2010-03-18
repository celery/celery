#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs
import platform

try:
    from setuptools import setup, find_packages, Command
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages, Command

import celery as distmeta


class RunTests(Command):
    description = "Run the django test suite from the tests dir."
    user_options = []
    extra_env = {}

    def run(self):
        for env_name, env_value in self.extra_env.items():
            os.environ[env_name] = str(env_value)

        this_dir = os.getcwd()
        testproj_dir = os.path.join(this_dir, "tests")
        os.chdir(testproj_dir)
        sys.path.append(testproj_dir)
        from django.core.management import execute_manager
        os.environ["DJANGO_SETTINGS_MODULE"] = os.environ.get(
                        "DJANGO_SETTINGS_MODULE", "settings")
        settings_file = os.environ["DJANGO_SETTINGS_MODULE"]
        settings_mod = __import__(settings_file, {}, {}, [''])
        execute_manager(settings_mod, argv=[
            __file__, "test"])
        os.chdir(this_dir)

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


class QuickRunTests(RunTests):
    extra_env = dict(SKIP_RLIMITS=1, QUICKTEST=1)

install_requires = []

try:
    import django
except ImportError:
    install_requires.append("django")

try:
    import importlib
except ImportError:
    install_requires.append("importlib")


install_requires.extend([
    "python-dateutil",
    "anyjson",
    "carrot>=0.10.3",
    "django-picklefield",
    "billiard>=0.2.1"])

py_version = sys.version_info
if sys.version_info <= (2, 5):
    install_requires.append("multiprocessing==2.6.2.1")
if sys.version_info <= (2, 4):
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
    scripts=["bin/celeryd", "bin/celeryinit", "bin/celerybeat", "bin/camqadm"],
    zip_safe=False,
    install_requires=install_requires,
    cmdclass = {"test": RunTests, "quicktest": QuickRunTests},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
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
    entry_points={
        'console_scripts': [
            'celeryd = celery.bin.celeryd:main',
            'celeryinit = celery.bin.celeryinit:main',
            'celerybeat = celery.bin.celerybeat:main',
            'camqadm = celery.bin.camqadm:main',
            ],
    },
    long_description=long_description,
)
