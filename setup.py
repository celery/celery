#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs
import platform

extra = {}
tests_require = ["nose", "nose-cover3", "sqlalchemy"]
if sys.version_info >= (3, 0):
    extra.update(use_2to3=True)
elif sys.version_info < (2, 7):
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

os.environ["CELERY_NO_EVAL"] = "yes"
import celery as distmeta
os.environ.pop("CELERY_NO_EVAL", None)
sys.modules.pop("celery", None)


class quicktest(test):
    extra_env = dict(SKIP_RLIMITS=1, QUICKTEST=1)

    def run(self, *args, **kwargs):
        for env_name, env_value in self.extra_env.items():
            os.environ[env_name] = str(env_value)
        test.run(self, *args, **kwargs)

install_requires = []
try:
    import importlib
except ImportError:
    install_requires.append("importlib")
install_requires.extend([
    "python-dateutil>=1.5.0,<2.0.0",
    "anyjson>=0.3.1",
    "kombu>=1.1.2,<2.0.0",
    "pyparsing>=1.5.0,<2.0.0",
])
py_version = sys.version_info
is_jython = sys.platform.startswith("java")
is_pypy = hasattr(sys, "pypy_version_info")
if sys.version_info < (2, 6) and not (is_jython or is_pypy):
    install_requires.append("multiprocessing")
if sys.version_info < (2, 5):
    install_requires.append("uuid")

if is_jython:
    install_requires.append("threadpool")
    install_requires.append("simplejson")

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
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    cmdclass={"test": test,
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
