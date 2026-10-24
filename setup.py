#!/usr/bin/env python3
import os

import setuptools

# -*- Extras -*-

EXTENSIONS = (
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
    'gcpubsub',
    'gevent',
    'gcs',
    'librabbitmq',
    'memcache',
    'mongodb',
    'msgpack',
    'pymemcache',
    'pydantic',
    'pyro',
    'pytest',
    'redis',
    's3',
    'slmq',
    'solar',
    'sqlalchemy',
    'sqs',
    'tblib',
    'yaml',
    'zookeeper',
    'zstd'
)

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


setuptools.setup(
    install_requires=install_requires(),
    extras_require=extras_require(),
)
