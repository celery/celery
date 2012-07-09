#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs

if sys.version_info < (2, 5):
    raise Exception('Celery requires Python 2.5 or higher.')

try:
    orig_path = sys.path[:]
    for path in (os.path.curdir, os.getcwd()):
        if path in sys.path:
            sys.path.remove(path)
    try:
        import celery.app
        import imp
        import shutil
        _, task_path, _ = imp.find_module('task', celery.app.__path__)
        if task_path.endswith('/task'):
            print('- force upgrading previous installation')
            print('  - removing %r package...' % task_path)
            try:
                shutil.rmtree(os.path.abspath(task_path))
            except Exception:
                sys.stderr.write('Could not remove %r: %r\n' % (
                    task_path, sys.exc_info[1]))
    except ImportError:
        print('Upgrade: no old version found.')
    finally:
        sys.path[:] = orig_path
except:
    pass


try:
    from setuptools import setup, find_packages
    from setuptools.command.test import test
except ImportError:
    raise
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages           # noqa
    from setuptools.command.test import test              # noqa

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
    Programming Language :: Python :: 2.5
    Programming Language :: Python :: 2.6
    Programming Language :: Python :: 2.7
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
is_py3k = sys.version_info[0] == 3
if is_py3k:
    extra.update(use_2to3=True)

# -*- Distribution Meta -*-

import re
re_meta = re.compile(r'__(\w+?)__\s*=\s*(.*)')
re_vers = re.compile(r'VERSION\s*=\s*\((.*?)\)')
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

# -*- Custom Commands -*-


class quicktest(test):
    extra_env = dict(SKIP_RLIMITS=1, QUICKTEST=1)

    def run(self, *args, **kwargs):
        for env_name, env_value in self.extra_env.items():
            os.environ[env_name] = str(env_value)
        test.run(self, *args, **kwargs)

# -*- Installation Requires -*-

py_version = sys.version_info
is_jython = sys.platform.startswith('java')
is_pypy = hasattr(sys, 'pypy_version_info')


def strip_comments(l):
    return l.split('#', 1)[0].strip()


def reqs(f):
    return filter(None, [strip_comments(l) for l in open(
        os.path.join(os.getcwd(), 'requirements', f)).readlines()])

install_requires = reqs('default-py3k.txt' if is_py3k else 'default.txt')

if is_jython:
    install_requires.extend(reqs('jython.txt'))
if py_version[0:2] == (2, 6):
    install_requires.extend(reqs('py26.txt'))
elif py_version[0:2] == (2, 5):
    install_requires.extend(reqs('py25.txt'))

# -*- Tests Requires -*-

if is_py3k:
    tests_require = reqs('test-py3k.txt')
elif is_pypy:
    tests_require = reqs('test-pypy.txt')
else:
    tests_require = reqs('test.txt')

if py_version[0:2] == (2, 5):
    tests_require.extend(reqs('test-py25.txt'))

# -*- Long Description -*-

if os.path.exists('README.rst'):
    long_description = codecs.open('README.rst', 'r', 'utf-8').read()
else:
    long_description = 'See http://pypi.python.org/pypi/celery'

# -*- Entry Points -*- #

console_scripts = entrypoints['console_scripts'] = [
        'celery = celery.bin.celery:main',
        'celeryd = celery.bin.celeryd:main',
        'celerybeat = celery.bin.celerybeat:main',
        'camqadm = celery.bin.camqadm:main',
        'celeryev = celery.bin.celeryev:main',
        'celeryctl = celery.bin.celeryctl:main',
        'celeryd-multi = celery.bin.celeryd_multi:main',
]

# bundles: Only relevant for Celery developers.
entrypoints['bundle.bundles'] = ['celery = celery.contrib.bundles:bundles']

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
    cmdclass={'quicktest': quicktest},
    classifiers=classifiers,
    entry_points=entrypoints,
    long_description=long_description,
    **extra)
