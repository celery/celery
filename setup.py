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


install_requires = ["django-unittest-depth",
                    "anyjson",
                    "carrot>=0.5.2"]

# python-daemon doesn't run on windows, so check current platform
if sys.platform == "win32":
    print
    print "I see you are using windows. You will not be able to run celery in daemon mode with the --detach parameter."
    print
else:
    install_requires.append("python-daemon")

py_version_info = sys.version_info
py_major_version = py_version_info[0]
py_minor_version = py_version_info[1]

if (py_major_version == 2 and py_minor_version <=5) or py_major_version < 2:
    install_requires.append("multiprocessing")

if os.path.exists("README.rst"):
    long_description = codecs.open("README.rst", "r", "utf-8").read()
else:
    long_description = "See http://pypi.python.org/pypi/celery"


setup(
    name='celery',
    version=celery.__version__,
    description=celery.__doc__,
    author=celery.__author__,
    author_email=celery.__contact__,
    url=celery.__homepage__,
    platforms=["any"],
    packages=find_packages(exclude=['ez_setup']),
    scripts=["bin/celeryd", "bin/celeryinit"],
    zip_safe=False,
    install_requires=install_requires,
    extra_requires={
        "Tyrant": ["pytyrant"],
    },
    cmdclass = {"test": RunTests},
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
    long_description=long_description,
)
