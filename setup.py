#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
import os
import re
import sys

import setuptools
import setuptools.command.test

try:
    from platform import python_implementation as _pyimp
except (AttributeError, ImportError):
    def _pyimp():
        return 'Python (unknown)'

NAME = 'celery'

# -*- Python Versions -*-

E_UNSUPPORTED_PYTHON = """
----------------------------------------
 Celery 4.0 requires %s %s or later
----------------------------------------

- For CPython 2.6, PyPy 1.x, Jython 2.6, CPython 3.2->3.3; use Celery 3.1:

    $ pip install 'celery<4'

- For CPython 2.5, Jython 2.5; use Celery 3.0:

    $ pip install 'celery<3.1'

- For CPython 2.4; use Celery 2.2:

    $ pip install 'celery<2.3'
"""

PYIMP = _pyimp()
PY26_OR_LESS = sys.version_info < (2, 7)
PY3 = sys.version_info[0] == 3
PY34_OR_LESS = PY3 and sys.version_info < (3, 5)
PYPY_VERSION = getattr(sys, 'pypy_version_info', None)
PYPY = PYPY_VERSION is not None
PYPY24_ATLEAST = PYPY_VERSION and PYPY_VERSION >= (2, 4)

if PY26_OR_LESS:
    raise Exception(E_UNSUPPORTED_PYTHON % (PYIMP, '2.7'))
elif PY34_OR_LESS and not PYPY24_ATLEAST:
    raise Exception(E_UNSUPPORTED_PYTHON % (PYIMP, '3.5'))

# -*- Extras -*-

EXTENSIONS = {
    'arangodb',
    'auth',
    'azureblockblob',
    'brotli',
    'cassandra',
    'consul',
    'cosmosdbsql',
    'couchbase',
    'couchdb',
    'django',
    'dynamodb',
    'elasticsearch',
    'eventlet',
    'gevent',
    'librabbitmq',
    'lzma',
    'memcache',
    'mongodb',
    'msgpack',
    'pymemcache',
    'pyro',
    'redis',
    'riak',
    's3',
    'slmq',
    'solar',
    'sqlalchemy',
    'sqs',
    'tblib',
    'yaml',
    'zookeeper',
    'zstd'
}

# -*- Distribution Meta -*-

re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_doc = re.compile(r'^"""(.+?)"""')


def _add_default(m):
    attr_name, attr_value = m.groups()
    return ((attr_name, attr_value.strip("\"'")),)


def _add_doc(m):
    return (('doc', m.groups()[0]),)


def parse_dist_meta():
    """Extract metadata information from ``$dist/__init__.py``."""
    pats = {re_meta: _add_default, re_doc: _add_doc}
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, NAME, '__init__.py')) as meta_fh:
        distmeta = {}
        for line in meta_fh:
            if line.strip() == '# -eof meta-':
                break
            for pattern, handler in pats.items():
                m = pattern.match(line.strip())
                if m:
                    distmeta.update(handler(m))
        return distmeta

# -*- Requirements -*-


def _strip_comments(l):
    return l.split('#', 1)[0].strip()


def _pip_requirement(req):
    if req.startswith('-r '):
        _, path = req.split()
        return reqs(*path.split('/'))
    return [req]


def _reqs(*f):
    return [
        _pip_requirement(r) for r in (
            _strip_comments(l) for l in open(
                os.path.join(os.getcwd(), 'requirements', *f)).readlines()
        ) if r]


def reqs(*f):
    """Parse requirement file.

    Example:
        reqs('default.txt')          # requirements/default.txt
        reqs('extras', 'redis.txt')  # requirements/extras/redis.txt
    Returns:
        List[str]: list of requirements specified in the file.
    """
    return [req for subreq in _reqs(*f) for req in subreq]


def extras(*p):
    """Parse requirement in the requirements/extras/ directory."""
    return reqs('extras', *p)


def install_requires():
    """Get list of requirements required for installation."""
    return reqs('default.txt')


def extras_require():
    """Get map of all extra requirements."""
    return {x: extras(x + '.txt') for x in EXTENSIONS}

# -*- Long Description -*-


def long_description():
    try:
        return codecs.open('README.rst', 'r', 'utf-8').read()
    except IOError:
        return 'Long description error: Missing README.rst file'

# -*- Command: setup.py test -*-


class pytest(setuptools.command.test.test):
    user_options = [('pytest-args=', 'a', 'Arguments to pass to py.test')]

    def initialize_options(self):
        setuptools.command.test.test.initialize_options(self)
        self.pytest_args = []

    def run_tests(self):
        import pytest as _pytest
        sys.exit(_pytest.main(self.pytest_args))

# -*- %%% -*-


meta = parse_dist_meta()
setuptools.setup(
    name=NAME,
    packages=setuptools.find_packages(exclude=['t', 't.*']),
    version=meta['version'],
    description=meta['doc'],
    long_description=long_description(),
    keywords=meta['keywords'],
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    license='BSD',
    platforms=['any'],
    install_requires=install_requires(),
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*",
    tests_require=reqs('test.txt'),
    extras_require=extras_require(),
    cmdclass={'test': pytest},
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'celery = celery.__main__:main',
        ],
        'pytest11': [
            'celery = celery.contrib.pytest',
        ],
    },
    project_urls={
        "Documentation": "http://docs.celeryproject.org/en/latest/index.html",
        "Code": "https://github.com/celery/celery",
        "Tracker": "https://github.com/celery/celery/issues",
        "Funding": "https://opencollective.com/celery"
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: BSD License",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Object Brokering",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Operating System :: OS Independent"
    ]
)
