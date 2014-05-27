#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

import os
import sys
import codecs

CELERY_COMPAT_PROGRAMS = int(os.environ.get('CELERY_COMPAT_PROGRAMS', 1))

if sys.version_info < (2, 7):
    raise Exception('Celery 3.2 requires Python 2.7 or higher.')

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

PY3 = sys.version_info[0] == 3
JYTHON = sys.platform.startswith('java')
PYPY = hasattr(sys, 'pypy_version_info')

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
    Programming Language :: Python :: 3.3
    Programming Language :: Python :: 3.4
    Programming Language :: Python :: Implementation :: CPython
    Programming Language :: Python :: Implementation :: PyPy
    Programming Language :: Python :: Implementation :: Jython
    Operating System :: OS Independent
"""
classifiers = [s.strip() for s in classes.split('\n') if s]

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

# -*- Extras -*-

extras = lambda *p: reqs('extras', *p)
# Celery specific
specific_list = ['auth', 'cassandra', 'memcache', 'couchbase', 'threads',
                 'eventlet', 'gevent', 'msgpack', 'yaml', 'redis',
                 'mongodb', 'sqs', 'couchdb', 'beanstalk', 'zookeeper',
                 'zeromq', 'sqlalchemy', 'librabbitmq', 'pyro', 'slmq']
extras_require = dict((x, extras(x + '.txt')) for x in specific_list)
extra['extras_require'] = extras_require

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
    include_package_data=False,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    test_suite='nose.collector',
    classifiers=classifiers,
    entry_points=entrypoints,
    long_description=long_description,
    **extra)
