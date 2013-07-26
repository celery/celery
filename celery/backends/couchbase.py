# -*- coding: utf-8 -*-
"""
    celery.backends.couchbase
    ~~~~~~~~~~~~~~~~~~~~~~~

    CouchBase result store backend.

"""
from __future__ import absolute_import

from datetime import datetime

try:
    import couchbase
    from couchbase import Couchbase 
    from couchbase.connection import Connection
    from couchbase.user_constants import *
    import couchbase._libcouchbase as _LCB
except ImportError:
    couchbase = None   # noqa

from kombu.utils import cached_property
from kombu.utils.url import _parse_url

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.five import string_t
from celery.utils.timeutils import maybe_timedelta

from .base import KeyValueStoreBackend
import logging

# class Bunch(object):

#     def __init__(self, **kw):
#         self.__dict__.update(kw)

def is_single_url(obj):
    try:
        return isinstance(obj, basestring)
    except NameError:
        return isinstance(obj, str)

class CouchBaseBackend(KeyValueStoreBackend):

    bucket = "default"
    host = 'localhost'
    port = 8091
    username = None
    password = None
    quiet=False
    conncache=None
    unlock_gil=True
    timeout=2.5
    transcoder=None
    # supports_autoexpire = False
    
    def __init__(self, url=None, *args, **kwargs):
        """Initialize CouchBase backend instance.

        :raises celery.exceptions.ImproperlyConfigured: if
            module :mod:`pymongo` is not available.

        """
        super(CouchBaseBackend, self).__init__(*args, **kwargs)

        self.expires = kwargs.get('expires') or maybe_timedelta(
            self.app.conf.CELERY_TASK_RESULT_EXPIRES)

        if not couchbase:
            raise ImproperlyConfigured(
                'You need to install the couchbase library to use the '
                'CouchBase backend.')

        uhost = uport = uname = upass = ubucket = None
        if url:
            _, uhost, uport, uname, upass, ubucket, _ = _parse_url(url)
            ubucket = ubucket.strip('/') if ubucket else 'default_bucket'

        config = self.app.conf.get('CELERY_COUCHBASE_BACKEND_SETTINGS', None)
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'Couchbase backend settings should be grouped in a dict')
        else:
            config = {}
            
        self.host = uhost or config.get('host', self.host)
        self.port = int(uport or config.get('port', self.port))
        self.bucket = ubucket or config.get('bucket', self.bucket)
        self.username = uname or config.get('username', self.username)
        self.password = upass or config.get('password', self.password)
        # self.quiet = config.get('quiet', self.quiet)
        # self.conncache = config.get('conncache', self.conncache)
        # self.unlock_gil = config.get('unlock_gil', self.unlock_gil)
        # self.timeout = config.get('timeout', self.timeout)
        # self.transcoder = config.get('transcoder', self.transcoder)
        # self.lockmode = config.get('lockmode', self.lockmode)
            
        self._connection = None

    def _get_connection(self):
        """Connect to the MongoDB server."""
        if self._connection is None:
            kwargs = {
                'bucket': self.bucket,
                'host': self.host
            }

            if self.port:
                kwargs.update({'port': self.port})
            if self.username:
                kwargs.update({'username': self.username})
            if self.password:
                kwargs.update({'password': self.password})

            logging.debug("couchbase settings %s" % kwargs)
            self._connection = Connection(
                **dict(kwargs)
            )
        return self._connection

    @property
    def connection(self):
        return self._get_connection()

    def get(self, key):
        try:
            if self._connection == None:
                self._connection = self._get_connection()
            return self._connection.get(key).value
        except:
            return None

    def set(self, key, value):
        if self._connection == None:
            self._connection = self._get_connection()
        self._connection.set(key, value)

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def delete(self, key):
        if self._connection == None:
            self._connection = self._get_connection()
        self._connection.delete(key)
