from __future__ import absolute_import, unicode_literals

from case import Mock, skip
from celery.backends.consul import ConsulBackend


@skip.unless_module('consul')
class test_ConsulBackend:

    def setup(self):
        self.backend = ConsulBackend(
            app=self.app, url='consul://localhost:800')

    def test_supports_autoexpire(self):
        assert self.backend.supports_autoexpire

    def test_consul_consistency(self):
        assert self.backend.consistency == 'consistent'

    def test_get(self):
        index = 100
        data = {'Key': 'test-consul-1', 'Value': 'mypayload'}
        self.backend.client = Mock(name='c.client')
        self.backend.client.kv.get.return_value = (index, data)
        assert self.backend.get(data['Key']) == 'mypayload'

    def test_index_bytes_key(self):
        key = 'test-consul-2'
        assert self.backend._key_to_consul_key(key) == key
        assert self.backend._key_to_consul_key(key.encode('utf-8')) == key
