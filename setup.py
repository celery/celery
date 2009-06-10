#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
import sys
import os

try:
    from setuptools import setup, find_packages, Command
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages, Command

import celery


class RunTests(Command):
    description = "Run the django test suite from the testproj dir."

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        this_dir = os.getcwd()
        testproj_dir = os.path.join(this_dir, "testproj")
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


install_requires = ["carrot", "django"]
py_version_info = sys.version_info
py_major_version = py_version_info[0]
py_minor_version = py_version_info[1]

if (py_major_version == 2 and py_minor_version <=5) or py_major_version < 2:
    install_requires.append("multiprocessing")

long_description = codecs.open("README", "r", "utf-8").read()


setup(
    name='celery',
    version=celery.__version__,
    description=celery.__doc__,
    author=celery.__author__,
    author_email=celery.__contact__,
    url=celery.__homepage__,
    platforms=["any"],
    packages=find_packages(exclude=['ez_setup']),
    scripts=["bin/celeryd"],
    zip_safe=False,
    install_requires=[
        'carrot>=0.4.1',
        'django',
    ],
    cmdclass = {"test": RunTests},
    classifiers=[
        "Development Status :: 4 - Beta",
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
    long_description=long_description,
)
