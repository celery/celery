#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

import os
import re
import sys
import codecs

try:
    import platform
    _pyimp = platform.python_implementation
except (AttributeError, ImportError):
    def _pyimp():
        return 'Python'

E_UNSUPPORTED_PYTHON = """
----------------------------------------
 Celery 4.0 requires %s %s or later!
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
PY33_OR_LESS = PY3 and sys.version_info < (3, 4)
JYTHON = sys.platform.startswith('java')
PYPY_VERSION = getattr(sys, 'pypy_version_info', None)
PYPY = PYPY_VERSION is not None
PYPY24_ATLEAST = PYPY_VERSION and PYPY_VERSION >= (2, 4)

if PY26_OR_LESS:
    raise Exception(E_UNSUPPORTED_PYTHON % (PYIMP, '2.7'))
elif PY33_OR_LESS and not PYPY24_ATLEAST:
    raise Exception(E_UNSUPPORTED_PYTHON % (PYIMP, '3.4'))

# -*- Upgrading from older versions -*-

downgrade_packages = [
    'celery.app.task',
]
orig_path = sys.path[:]
for path in (os.path.curdir, os.getcwd()):
    if path in sys.path:
        sys.path.remove(path)
try:
    import imp
    import shutil
    for pkg in downgrade_packages:
        try:
            parent, module = pkg.rsplit('.', 1)
            print('- Trying to upgrade %r in %r' % (module, parent))
            parent_mod = __import__(parent, None, None, [parent])
            _, mod_path, _ = imp.find_module(module, parent_mod.__path__)
            if mod_path.endswith('/' + module):
                print('- force upgrading previous installation')
                print('  - removing {0!r} package...'.format(mod_path))
                try:
                    shutil.rmtree(os.path.abspath(mod_path))
                except Exception:
                    sys.stderr.write('Could not remove {0!r}: {1!r}\n'.format(
                        mod_path, sys.exc_info[1]))
        except ImportError:
            print('- upgrade %s: no old version found.' % module)
except:
    pass
finally:
    sys.path[:] = orig_path

NAME = 'celery'
entrypoints = {}
extra = {}

# -*- Classifiers -*-

classes = """
    Development Status :: 5 - Production/Stable
    License :: OSI Approved :: BSD License
    Topic :: System :: Distributed Computing
    Topic :: Software Development :: Object Brokering
    Programming Language :: Python
    Programming Language :: Python :: 2
    Programming Language :: Python :: 2.7
    Programming Language :: Python :: 3
    Programming Language :: Python :: 3.4
    Programming Language :: Python :: 3.5
    Programming Language :: Python :: Implementation :: CPython
    Programming Language :: Python :: Implementation :: PyPy
    Operating System :: OS Independent
"""
classifiers = [s.strip() for s in classes.split('\n') if s]

# -*- Distribution Meta -*-

re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_doc = re.compile(r'^"""(.+?)"""')


def add_default(m):
    attr_name, attr_value = m.groups()
    return ((attr_name, attr_value.strip("\"'")),)


def add_doc(m):
    return (('doc', m.groups()[0]),)

pats = {re_meta: add_default, re_doc: add_doc}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'celery/__init__.py')) as meta_fh:
    meta = {}
    for line in meta_fh:
        if line.strip() == '# -eof meta-':
            break
        for pattern, handler in pats.items():
            m = pattern.match(line.strip())
            if m:
                meta.update(handler(m))

# -*- Installation Requires -*-


def strip_comments(l):
    return l.split('#', 1)[0].strip()


def _pip_requirement(req):
    if req.startswith('-r '):
        _, path = req.split()
        return reqs(*path.split('/'))
    return [req]


def _reqs(*f):
    return [
        _pip_requirement(r) for r in (
            strip_comments(l) for l in open(
                os.path.join(os.getcwd(), 'requirements', *f)).readlines()
        ) if r]


def reqs(*f):
    return [req for subreq in _reqs(*f) for req in subreq]

install_requires = reqs('default.txt')
if JYTHON:
    install_requires.extend(reqs('jython.txt'))

# -*- Long Description -*-

if os.path.exists('README.rst'):
    long_description = codecs.open('README.rst', 'r', 'utf-8').read()
else:
    long_description = 'See http://pypi.python.org/pypi/celery'

# -*- Entry Points -*- #

console_scripts = entrypoints['console_scripts'] = [
    'celery = celery.__main__:main',
]

# -*- Extras -*-


def extras(*p):
    return reqs('extras', *p)

# Celery specific
features = set([
    'auth', 'cassandra', 'elasticsearch', 'memcache', 'pymemcache',
    'couchbase', 'eventlet', 'gevent', 'msgpack', 'yaml',
    'redis', 'sqs', 'couchdb', 'riak', 'zookeeper',
    'sqlalchemy', 'librabbitmq', 'pyro', 'slmq', 'tblib', 'consul'
])
extras_require = dict((x, extras(x + '.txt')) for x in features)
extra['extras_require'] = extras_require

# -*- %%% -*-

setup(
    name=NAME,
    version=meta['version'],
    description=meta['doc'],
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    platforms=['any'],
    license='BSD',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    include_package_data=False,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=reqs('test.txt'),
    test_suite='nose.collector',
    classifiers=classifiers,
    entry_points=entrypoints,
    long_description=long_description,
    **extra)
