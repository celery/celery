"""
Keys and certificates for tests (KEY1 is a private key of CERT1, etc.)

Generated with:

.. code-block:: bash

    $ openssl genrsa -des3 -passout pass:test -out key1.key 1024
    $ openssl req -new -key key1.key -out key1.csr -passin pass:test
    $ cp key1.key key1.key.org
    $ openssl rsa -in key1.key.org -out key1.key -passin pass:test
    $ openssl x509 -req -days 365 -in cert1.csr \
              -signkey key1.key -out cert1.crt
    $ rm key1.key.org cert1.csr

"""
from __future__ import absolute_import
from __future__ import with_statement

import __builtin__

from mock import Mock, patch

from celery import current_app
from celery.exceptions import ImproperlyConfigured
from celery.security import setup_security, disable_untrusted_serializers
from kombu.serialization import registry

from .case import SecurityCase

from celery.tests.utils import mock_open


class test_security(SecurityCase):

    def tearDown(self):
        registry._disabled_content_types.clear()

    def test_disable_untrusted_serializers(self):
        disabled = registry._disabled_content_types
        self.assertEqual(0, len(disabled))

        disable_untrusted_serializers(
            ['application/json', 'application/x-python-serialize'])
        self.assertIn('application/x-yaml', disabled)
        self.assertNotIn('application/json', disabled)
        self.assertNotIn('application/x-python-serialize', disabled)
        disabled.clear()

        disable_untrusted_serializers()
        self.assertIn('application/x-yaml', disabled)
        self.assertIn('application/json', disabled)
        self.assertIn('application/x-python-serialize', disabled)

    def test_setup_security(self):
        disabled = registry._disabled_content_types
        self.assertEqual(0, len(disabled))

        current_app.conf.CELERY_TASK_SERIALIZER = 'json'

        setup_security()
        self.assertIn('application/x-python-serialize', disabled)
        disabled.clear()

    @patch('celery.security.register_auth')
    @patch('celery.security.disable_untrusted_serializers')
    def test_setup_registry_complete(self, dis, reg, key='KEY', cert='CERT'):
        calls = [0]

        def effect(*args):
            try:
                m = Mock()
                m.read.return_value = 'B' if calls[0] else 'A'
                return m
            finally:
                calls[0] += 1

        with mock_open(side_effect=effect):
            store = Mock()
            setup_security(['json'], key, cert, store)
            dis.assert_called_with(['json'])
            reg.assert_called_with('A', 'B', store)

    def test_security_conf(self):
        current_app.conf.CELERY_TASK_SERIALIZER = 'auth'

        self.assertRaises(ImproperlyConfigured, setup_security)

        _import = __builtin__.__import__

        def import_hook(name, *args, **kwargs):
            if name == 'OpenSSL':
                raise ImportError
            return _import(name, *args, **kwargs)

        __builtin__.__import__ = import_hook
        self.assertRaises(ImproperlyConfigured, setup_security)
        __builtin__.__import__ = _import
