# -*- coding: utf-8 -*-
"""
    celery.backends.consul
    ~~~~~~~~~~~~~~~~~~~~~

    Consul result store backend.

    - :class:`ConsulBackend` implements KeyValueStoreBackend to store results
      the key-value store of Consul.

"""
from __future__ import absolute_import, unicode_literals

from kombu.utils.url import parse_url

from celery.exceptions import ImproperlyConfigured
from celery.backends.base import KeyValueStoreBackend, PY3
from celery.utils.log import get_logger

try:
    import consul
except ImportError:
    consul = None

LOGGER = get_logger(__name__)

__all__ = ['ConsulBackend']

CONSUL_MISSING = """\
You need to install the python-consul library in order to use \
the Consul result store backend."""


class ConsulBackend(KeyValueStoreBackend):
    """
    Consul.io K/V store backend for Celery
    """
    consul = consul

    supports_autoexpire = True

    client = None
    consistency = 'consistent'
    path = None

    def __init__(self, url=None, expires=None, **kwargs):
        super(ConsulBackend, self).__init__(**kwargs)

        if self.consul is None:
            raise ImproperlyConfigured(CONSUL_MISSING)

        self.url = url
        self.expires = self.prepare_expires(expires, int)

        params = parse_url(self.url)
        self.path = params['virtual_host']
        LOGGER.debug('Setting on Consul client to connect to %s:%d',
                     params['hostname'], params['port'])
        self.client = consul.Consul(host=params['hostname'],
                                    port=params['port'],
                                    consistency=self.consistency)

    def _key_to_consul_key(self, key):
        if PY3:
            key = key.decode('utf-8')

        if self.path is not None:
            return '{0}/{1}'.format(self.path, key)

        return key

    def get(self, key):
        LOGGER.debug('Trying to fetch key %s from Consul',
                     self._key_to_consul_key(key))
        try:
            _, data = self.client.kv.get(self._key_to_consul_key(key))
            return data['Value']
        except TypeError:
            pass

    def mget(self, keys):
        for key in keys:
            yield self.get(key)

    def set(self, key, value):
        """Set a key in Consul

        Before creating the key it will create a session inside Consul
        where it creates a session with a TTL

        The key created afterwards will reference to the session's ID.

        If the session expires it will remove the key so that results
        can auto expire from the K/V store
        """
        session_name = key

        if PY3:
            session_name = key.decode('utf-8')

        LOGGER.debug('Trying to create Consul session %s with TTL %d',
                     session_name, self.expires)
        session_id = self.client.session.create(name=session_name,
                                                behavior='delete',
                                                ttl=self.expires)
        LOGGER.debug('Created Consul session %s', session_id)

        LOGGER.debug('Writing key %s to Consul', self._key_to_consul_key(key))
        return self.client.kv.put(key=self._key_to_consul_key(key),
                                  value=value,
                                  acquire=session_id)

    def delete(self, key):
        LOGGER.debug('Removing key %s from Consul',
                     self._key_to_consul_key(key))
        return self.client.kv.delete(self._key_to_consul_key(key))
