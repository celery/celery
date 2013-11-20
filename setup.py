#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup, find_packages
    from setuptools.command.test import test
    is_setuptools = True
except ImportError:
    raise
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages           # noqa
    from setuptools.command.test import test              # noqa
    is_setuptools = False

import os
import sys
import codecs

CELERY_COMPAT_PROGRAMS = int(os.environ.get('CELERY_COMPAT_PROGRAMS', 1))

if sys.version_info < (2, 6):
    raise Exception('Celery 3.1 requires Python 2.6 or higher.')

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
    Programming Language :: Python :: 2.6
    Programming Language :: Python :: 2.7
    Programming Language :: Python :: 3
    Programming Language :: Python :: 3.3
    Programming Language :: Python :: Implementation :: CPython
    Programming Language :: Python :: Implementation :: PyPy
    Programming Language :: Python :: Implementation :: Jython
    Operating System :: OS Independent
    Operating System :: POSIX
    Operating System :: Microsoft :: Windows
    Operating System :: MacOS :: MacOS X
"""
classifiers = [s.strip() for s in classes.split('\n') if s]

PY3 = sys.version_info[0] == 3
JYTHON = sys.platform.startswith('java')
PYPY = hasattr(sys, 'pypy_version_info')

# -*- Distribution Meta -*-

import re
re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_vers = re.compile(r'VERSION\s*=.*?\((.*?)\)')
re_doc = re.compile(r'^"""(.+?)"""')
rq = lambda s: s.strip("\"'")


def add_default(m):
    attr_name, attr_value = m.groups()
    return ((attr_name, rq(attr_value)), )


def add_version(m):
    v = list(map(rq, m.groups()[0].split(', ')))
    return (('VERSION', '.'.join(v[0:3]) + ''.join(v[3:])), )


def add_doc(m):
    return (('doc', m.groups()[0]), )

pats = {re_meta: add_default,
        re_vers: add_version,
        re_doc: add_doc}
here = os.path.abspath(os.path.dirname(__file__))
meta_fh = open(os.path.join(here, 'celery/__init__.py'))
try:
    meta = {}
    for line in meta_fh:
        if line.strip() == '# -eof meta-':
            break
        for pattern, handler in pats.items():
            m = pattern.match(line.strip())
            if m:
                meta.update(handler(m))
finally:
    meta_fh.close()

# -*- Installation Requires -*-

py_version = sys.version_info


def strip_comments(l):
    return l.split('#', 1)[0].strip()


def reqs(*f):
    return [
        r for r in (
            strip_comments(l) for l in open(
                os.path.join(os.getcwd(), 'requirements', *f)).readlines()
        ) if r]

install_requires = reqs('default.txt')
if JYTHON:
    install_requires.extend(reqs('jython.txt'))

# -*- Tests Requires -*-

tests_require = reqs('test3.txt' if PY3 else 'test.txt')

# -*- Long Description -*-

if os.path.exists('README.rst'):
    long_description = codecs.open('README.rst', 'r', 'utf-8').read()
else:
    long_description = 'See http://pypi.python.org/pypi/celery'

# -*- Entry Points -*- #

console_scripts = entrypoints['console_scripts'] = [
    'celery = celery.__main__:main',
]

if CELERY_COMPAT_PROGRAMS:
    console_scripts.extend([
        'celeryd = celery.__main__:_compat_worker',
        'celerybeat = celery.__main__:_compat_beat',
        'celeryd-multi = celery.__main__:_compat_multi',
    ])

if is_setuptools:
    extras = lambda *p: reqs('extras', *p)
    extra['extras_require'] = {
        # Celery specific
        'auth': extras('auth.txt'),
        'cassandra': extras('cassandra.txt'),
        'memcache': extras('memcache.txt'),
        'couchbase': extras('couchbase.txt'),
        'threads': extras('threads.txt'),
        'eventlet': extras('eventlet.txt'),
        'gevent': extras('gevent.txt'),

        'msgpack': extras('msgpack.txt'),
        'yaml': extras('yaml.txt'),
        'redis': extras('redis.txt'),
        'mongodb': extras('mongodb.txt'),
        'sqs': extras('sqs.txt'),
        'couchdb': extras('couchdb.txt'),
        'beanstalk': extras('beanstalk.txt'),
        'zookeeper': extras('zookeeper.txt'),
        'zeromq': extras('zeromq.txt'),
        'sqlalchemy': extras('sqlalchemy.txt'),
        'librabbitmq': extras('librabbitmq.txt'),
        'pyro': extras('pyro.txt'),
        'slmq': extras('slmq.txt'),
    }

# -*- %%% -*-

setup(
    name=NAME,
    version=meta['VERSION'],
    description=meta['doc'],
    author=meta['author'],
    author_email=meta['contact'],
    url=meta['homepage'],
    platforms=['any'],
    license='BSD',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    test_suite='nose.collector',
    classifiers=classifiers,
    entry_points=entrypoints,
    long_description=long_description,
    **extra)
