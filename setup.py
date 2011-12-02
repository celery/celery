#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs
import platform

if sys.version_info < (2, 5):
    raise Exception("Celery requires Python 2.5 or higher.")

try:
    from setuptools import setup, find_packages
    from setuptools.command.test import test
except ImportError:
    raise
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages           # noqa
    from setuptools.command.test import test              # noqa

NAME = "celery"
entrypoints = {}
extra = {}

# -*- Classifiers -*-

classes = """
    Development Status :: 5 - Production/Stable
    License :: OSI Approved :: BSD License
    Topic :: System :: Distributed Computing
    Topic :: Software Development :: Object Brokering
    Intended Audience :: Developers
    Intended Audience :: Information Technology
    Intended Audience :: Science/Research
    Intended Audience :: Financial and Insurance Industry
    Intended Audience :: Healthcare Industry
    Environment :: No Input/Output (Daemon)
    Environment :: Console
    Programming Language :: Python
    Programming Language :: Python :: 2
    Programming Language :: Python :: 2.5
    Programming Language :: Python :: 2.6
    Programming Language :: Python :: 2.7
    Programming Language :: Python :: 3
    Programming Language :: Python :: 3.2
    Programming Language :: Python :: Implementation :: CPython
    Programming Language :: Python :: Implementation :: PyPy
    Programming Language :: Python :: Implementation :: Jython
    Operating System :: OS Independent
    Operating System :: POSIX
    Operating System :: Microsoft :: Windows
    Operating System :: MacOS :: MacOS X
"""
classifiers = [s.strip() for s in classes.split('\n') if s]

# -*- Python 3 -*-
is_py3k  = sys.version_info >= (3, 0)
if is_py3k:
    extra.update(use_2to3=True)

# -*- Distribution Meta -*-

os.environ["CELERY_NO_EVAL"] = "yes"
import celery as distmeta
os.environ.pop("CELERY_NO_EVAL", None)
sys.modules.pop("celery", None)

# -*- Custom Commands -*-

class quicktest(test):
    extra_env = dict(SKIP_RLIMITS=1, QUICKTEST=1)

    def run(self, *args, **kwargs):
        for env_name, env_value in self.extra_env.items():
            os.environ[env_name] = str(env_value)
        test.run(self, *args, **kwargs)

# -*- Installation Dependencies -*-

install_requires = []
try:
    import importlib  # noqa
except ImportError:
    install_requires.append("importlib")
install_requires.extend([
    "anyjson>=0.3.1",
    "kombu>=1.4.3,<3.0.0",
])
if is_py3k:
    install_requires.append("python-dateutil>=2.0.0")
else:
    install_requires.append("python-dateutil>=1.5.0,<2.0.0")

py_version = sys.version_info
is_jython = sys.platform.startswith("java")
is_pypy = hasattr(sys, "pypy_version_info")
if sys.version_info < (2, 7):
    install_requires.append("ordereddict") # Replacement for the ordered dict
if sys.version_info < (2, 6) and not (is_jython or is_pypy):
    install_requires.append("multiprocessing")

if is_jython:
    install_requires.append("threadpool")
    install_requires.append("simplejson")

# -*- Tests Requires -*-

tests_require = ["nose", "nose-cover3", "sqlalchemy", "mock"]
if sys.version_info < (2, 7):
    tests_require.append("unittest2")
elif sys.version_info <= (2, 5):
    tests_require.append("simplejson")

# -*- Long Description -*-

if os.path.exists("README.rst"):
    long_description = codecs.open("README.rst", "r", "utf-8").read()
else:
    long_description = "See http://pypi.python.org/pypi/celery"

# -*- Entry Points -*- #

console_scripts = entrypoints["console_scripts"] = [
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

# bundles: Only relevant for Celery developers.
entrypoints["bundle.bundles"] = ["celery = celery.contrib.bundles:bundles"]

# -*- %%% -*-

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
    test_suite="nose.collector",
    cmdclass={"quicktest": quicktest},
    classifiers=classifiers,
    entry_points=entrypoints,
    long_description=long_description,
    **extra)
