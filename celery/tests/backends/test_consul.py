from __future__ import absolute_import, unicode_literals

from celery.tests.case import AppCase, Mock, skip
from celery.backends.consul import ConsulBackend


@skip.unless_module('consul')
class test_ConsulBackend(AppCase):

    def setup(self):
        self.backend = ConsulBackend(
            app=self.app, url='consul://localhost:800')

    def test_supports_autoexpire(self):
        self.assertTrue(self.backend.supports_autoexpire)

    def test_consul_consistency(self):
        self.assertEqual('consistent', self.backend.consistency)

    def test_get(self):
        index = 100
        data = {'Key': 'test-consul-1', 'Value': 'mypayload'}
        self.backend.client = Mock(name='c.client')
        self.backend.client.kv.get.return_value = (index, data)
        self.assertEqual(self.backend.get(data['Key']), 'mypayload')
