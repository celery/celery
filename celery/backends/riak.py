# -*- coding: utf-8 -*-
"""
    celery.backends.riak
    ~~~~~~~~~~~~~~~~~~~~~~~

    Riak result store backend.

"""
from __future__ import absolute_import, print_function

from datetime import datetime

try:
    import riak
    from riak import RiakClient, RiakNode
    from riak.resolver import last_written_resolver
except ImportError:  # pragma: no cover
    riak = None      # noqa

from kombu.utils.url import _parse_url

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.utils.timeutils import maybe_timedelta

from .base import KeyValueStoreBackend


class NonAsciiBucket(Exception):
    """ Bucket must ne ascii charchters only. """


class Validators(object):

    @classmethod
    def validate_riak_bucket_name(cls, bucket_name):
        try:
            bucket_name.decode('ascii')
        except UnicodeDecodeError as ude:
            return False
        return True


class RiakBackend(KeyValueStoreBackend):
    # TODO: allow using other protocols than protobuf ?
    #: default protocol used to connect to Riak, might be `http` or `pbc`
    protocol = 'pbc'

    #: default Riak bucket name (`default`)
    bucket_name = "default"

    #: default Riak server hostname (`localhost`)
    host = 'localhost'

    #: default Riak server port (8087)
    port = 8087

    # supports_autoexpire = False

    def __init__(self, host=None, port=None, bucket_name=None, protocol=None,
                 url=None, *args, **kwargs):
        """Initialize Riak backend instance.

        :raises celery.exceptions.ImproperlyConfigured: if
            module :mod:`riak` is not available.
        """
        super(RiakBackend, self).__init__(*args, **kwargs)

        self.expires = kwargs.get('expires') or maybe_timedelta(
            self.app.conf.CELERY_TASK_RESULT_EXPIRES)

        if not riak:
            raise ImproperlyConfigured(
                'You need to install the riak library to use the '
                'Riak backend.')

        uhost = uport = uname = upass = ubucket = None
        if url:
            uprot, uhost, uport, uname, upass, ubucket, _ = _parse_url(url)
            if ubucket:
                ubucket = ubucket.strip('/')

        config = self.app.conf.get('CELERY_RIAK_BACKEND_SETTINGS', None)
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'Riak backend settings should be grouped in a dict')
        else:
            config = {}

        self.host = uhost or config.get('host', self.host)
        self.port = int(uport or config.get('port', self.port))
        self.bucket_name = ubucket or config.get('bucket', self.bucket_name)
        self.protocol = uprot or config.get('protocol', self.protocol)

        # riak bucket must be ascii letters or numbers only
        if not Validators.validate_riak_bucket_name(self.bucket_name):
            raise NonAsciiBucket("Riak bucket names must be ASCII characters")

        self._client = None

    def _get_client(self):
        """Get client connection"""
        if self._client is None or not self._client.is_alive():
            self._client = RiakClient(protocol=self.protocol,
                                      host=self.host,
                                      pb_port=self.port)
            self._client.resolver = last_written_resolver
        return self._client

    def _get_bucket(self):
        """Connect to our bucket"""
        if (
            self._client is None or not self._client.is_alive()
            or not self._bucket
        ):
            self._bucket = self.client.bucket(self.bucket_name)
        return self._bucket

    @property
    def client(self):
        return self._get_client()

    @property
    def bucket(self):
        return self._get_bucket()

    def get(self, key):
        return self.bucket.get(key).data

    def set(self, key, value):
        # RiakBucket.new(key=None, data=None, content_type='application/json',
        # encoded_data=None)
        _key = self.bucket.new(key, data=value)
        _key.store()

    def mget(self, keys):
        return [self.get(key).data for key in keys]

    def delete(self, key):
        self.bucket.delete(key)
