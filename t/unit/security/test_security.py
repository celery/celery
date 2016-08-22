"""Keys and certificates for tests (KEY1 is a private key of CERT1, etc.)

Generated with:

.. code-block:: console

    $ openssl genrsa -des3 -passout pass:test -out key1.key 1024
    $ openssl req -new -key key1.key -out key1.csr -passin pass:test
    $ cp key1.key key1.key.org
    $ openssl rsa -in key1.key.org -out key1.key -passin pass:test
    $ openssl x509 -req -days 365 -in cert1.csr \
              -signkey key1.key -out cert1.crt
    $ rm key1.key.org cert1.csr
"""
from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock, mock, patch
from kombu.serialization import disable_insecure_serializers

from celery.exceptions import ImproperlyConfigured, SecurityError
from celery.five import builtins
from celery.security import disable_untrusted_serializers, setup_security
from celery.security.utils import reraise_errors
from kombu.serialization import registry

from .case import SecurityCase


class test_security(SecurityCase):

    def teardown(self):
        registry._disabled_content_types.clear()

    def test_disable_insecure_serializers(self):
        try:
            disabled = registry._disabled_content_types
            assert disabled

            disable_insecure_serializers(
                ['application/json', 'application/x-python-serialize'],
            )
            assert 'application/x-yaml' in disabled
            assert 'application/json' not in disabled
            assert 'application/x-python-serialize' not in disabled
            disabled.clear()

            disable_insecure_serializers(allowed=None)
            assert 'application/x-yaml' in disabled
            assert 'application/json' in disabled
            assert 'application/x-python-serialize' in disabled
        finally:
            disable_insecure_serializers(allowed=['json'])

    @patch('celery.security._disable_insecure_serializers')
    def test_disable_untrusted_serializers(self, disable):
        disable_untrusted_serializers(['foo'])
        disable.assert_called_with(allowed=['foo'])

    def test_setup_security(self):
        disabled = registry._disabled_content_types
        assert len(disabled) == 0

        self.app.conf.task_serializer = 'json'
        self.app.setup_security()
        assert 'application/x-python-serialize' in disabled
        disabled.clear()

    @patch('celery.current_app')
    def test_setup_security__default_app(self, current_app):
        setup_security()

    @patch('celery.security.register_auth')
    @patch('celery.security._disable_insecure_serializers')
    def test_setup_registry_complete(self, dis, reg, key='KEY', cert='CERT'):
        calls = [0]

        def effect(*args):
            try:
                m = Mock()
                m.read.return_value = 'B' if calls[0] else 'A'
                return m
            finally:
                calls[0] += 1

        self.app.conf.task_serializer = 'auth'
        with mock.open(side_effect=effect):
            with patch('celery.security.registry') as registry:
                store = Mock()
                self.app.setup_security(['json'], key, cert, store)
                dis.assert_called_with(['json'])
                reg.assert_called_with('A', 'B', store, 'sha1', 'json')
                registry._set_default_serializer.assert_called_with('auth')

    def test_security_conf(self):
        self.app.conf.task_serializer = 'auth'
        with pytest.raises(ImproperlyConfigured):
            self.app.setup_security()

        _import = builtins.__import__

        def import_hook(name, *args, **kwargs):
            if name == 'OpenSSL':
                raise ImportError
            return _import(name, *args, **kwargs)

        builtins.__import__ = import_hook
        with pytest.raises(ImproperlyConfigured):
            self.app.setup_security()
        builtins.__import__ = _import

    def test_reraise_errors(self):
        with pytest.raises(SecurityError):
            with reraise_errors(errors=(KeyError,)):
                raise KeyError('foo')
        with pytest.raises(KeyError):
            with reraise_errors(errors=(ValueError,)):
                raise KeyError('bar')
