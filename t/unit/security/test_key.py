from __future__ import absolute_import, unicode_literals

import pytest

from celery.exceptions import SecurityError
from celery.security.key import PrivateKey

from . import CERT1, KEY1, KEY2
from .case import SecurityCase


class test_PrivateKey(SecurityCase):

    def test_valid_private_key(self):
        PrivateKey(KEY1)
        PrivateKey(KEY2)

    def test_invalid_private_key(self):
        with pytest.raises((SecurityError, TypeError)):
            PrivateKey(None)
        with pytest.raises(SecurityError):
            PrivateKey('')
        with pytest.raises(SecurityError):
            PrivateKey('foo')
        with pytest.raises(SecurityError):
            PrivateKey(KEY1[:20] + KEY1[21:])
        with pytest.raises(SecurityError):
            PrivateKey(CERT1)

    def test_sign(self):
        pkey = PrivateKey(KEY1)
        pkey.sign('test', b'sha1')
        with pytest.raises(ValueError):
            pkey.sign('test', b'unknown')
