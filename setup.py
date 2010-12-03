#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs
import platform

extra = {}
tests_require = ["nose", "nose-cover3"]
if sys.version_info >= (3, 0):
    extra.update(use_2to3=True)
elif sys.version_info <= (2, 6):
    tests_require.append("unittest2")
elif sys.version_info <= (2, 5):
    tests_require.append("simplejson")

if sys.version_info < (2, 4):
    raise Exception("Celery requires Python 2.4 or higher.")

try:
    from setuptools import setup, find_packages, Command
    from setuptools.command.test import test
    from setuptools.command.install import install
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages, Command
    from setuptools.command.test import test
    from setuptools.command.install import install

import celery as distmeta


def with_dist_not_in_path(fun):

    def _inner(*args, **kwargs):
        cwd = os.getcwd()
        removed = []
        for path in (cwd, cwd + "/", "."):
            try:
                i = sys.path.index(path)
            except ValueError:
                pass
            else:
                removed.append((i, path))
                sys.path.remove(path)

        try:
            dist_module = sys.modules.pop("celery", None)
            try:
                import celery as existing_module
            except ImportError:
                pass
            else:
                kwargs["celery"] = existing_module
                return fun(*args, **kwargs)
        finally:
            for i, path in removed:
                sys.path.insert(i, path)
            if dist_module:
                sys.modules["celery"] = dist_module

    return _inner


class Upgrade(object):
    old_modules = ("platform", )

    def run(self, dist=False):
        detect_ = self.detect_existing_installation
        if not dist:
            detect = with_dist_not_in_path(detect_)
        else:
            detect = lambda: detect_(distmeta)
        path = detect()
        if path:
            self.remove_modules(path)

    def detect_existing_installation(self, celery=None):
        path = os.path.dirname(celery.__file__)
        sys.stderr.write("* Upgrading old Celery from: \n\t%r\n" % path)
        return path

    def try_remove(self, file):
        try:
            os.remove(file)
        except OSError:
            pass

    def remove_modules(self, path):
        for module_name in self.old_modules:
            sys.stderr.write("* Removing old %s.py...\n" % module_name)
            self.try_remove(os.path.join(path, "%s.py" % module_name))
            self.try_remove(os.path.join(path, "%s.pyc" % module_name))


class mytest(test):

    def run(self, *args, **kwargs):
        Upgrade().run(dist=True)
        test.run(self, *args, **kwargs)


class quicktest(mytest):
    extra_env = dict(SKIP_RLIMITS=1, QUICKTEST=1)

    def run(self, *args, **kwargs):
        for env_name, env_value in self.extra_env.items():
            os.environ[env_name] = str(env_value)
        mytest.run(self, *args, **kwargs)


class upgrade(Command):
    user_options = []

    def run(self, *args, **kwargs):
        Upgrade().run()

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


class upgrade_and_install(install):

    def run(self, *args, **kwargs):
        Upgrade().run()
        install.run(self, *args, **kwargs)


install_requires = []

try:
    import importlib
except ImportError:
    install_requires.append("importlib")


install_requires.extend([
    "python-dateutil",
    "anyjson",
    "kombu>=0.9.1",
    "pyparsing",
])

py_version = sys.version_info
if sys.version_info < (2, 6) and not sys.platform.startswith("java"):
    install_requires.append("multiprocessing")
if sys.version_info < (2, 5):
    install_requires.append("uuid")

if os.path.exists("README.rst"):
    long_description = codecs.open("README.rst", "r", "utf-8").read()
else:
    long_description = "See http://pypi.python.org/pypi/celery"


console_scripts = [
        'celerybeat = celery.bin.celerybeat:main',
        'camqadm = celery.bin.camqadm:main',
        'celeryev = celery.bin.celeryev:main',
        'celeryctl = celery.bin.celeryctl:main',
        'celeryd-multi = celery.bin.celeryd_multi:main',
]

import platform
if platform.system() == "Windows":
    console_scripts.append('celeryd = celery.bin.celeryd:windows_main')
else:
    console_scripts.append('celeryd = celery.bin.celeryd:main')


setup(
    name="celery",
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
    tests_require=tests_require,
    cmdclass={"install": upgrade_and_install,
              "upgrade": upgrade,
              "test": mytest,
              "quicktest": quicktest},
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
        'console_scripts': console_scripts,
    },
    long_description=long_description,
    **extra
)
