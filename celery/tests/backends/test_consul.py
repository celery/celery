from __future__ import absolute_import, unicode_literals

from celery.tests.case import AppCase, Mock, skip
from celery.backends.consul import ConsulBackend

try:
    import consul
except ImportError:
    consul = None


@skip.unless_module('consul')
class test_ConsulBackend(AppCase):

    def setup(self):
        if consul is None:
            raise SkipTest('python-consul is not installed.')
        self.backend = ConsulBackend(app=self.app)

    def test_supports_autoexpire(self):
        self.assertTrue(self.backend.supports_autoexpire)

    def test_consul_consistency(self):
        self.assertEqual('consistent', self.backend.consistency)

    def test_get(self):
        c = ConsulBackend(app=self.app)
        c.client = Mock()
        c.client.kv = Mock()
        c.client.kv.get = Mock()
        index = 100
        data = {'Key': 'test-consul-1', 'Value': 'mypayload'}
        r = (index, data)
        c.client.kv.get.return_value = r
        i, d = c.get(data['Key'])
        self.assertEqual(i, 100)
        self.assertEqual(d['Key'], data['Key'])
