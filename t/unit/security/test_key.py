import pytest
from kombu.utils.encoding import ensure_bytes

from celery.exceptions import SecurityError
from celery.security.key import PrivateKey
from celery.security.utils import get_digest_algorithm

from . import CERT1, KEYPASSWORD, KEY1, ENCKEY1, KEY2, ENCKEY2
from .case import SecurityCase


class test_PrivateKey(SecurityCase):

    def test_valid_private_key(self):
        PrivateKey(KEY1)
        PrivateKey(KEY2)
        PrivateKey(ENCKEY1, KEYPASSWORD)
        PrivateKey(ENCKEY2, KEYPASSWORD)

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
            PrivateKey(ENCKEY1, KEYPASSWORD+b"wrong")
        with pytest.raises(SecurityError):
            PrivateKey(ENCKEY2, KEYPASSWORD+b"wrong")
        with pytest.raises(SecurityError):
            PrivateKey(CERT1)

    def test_sign(self):
        pkey = PrivateKey(KEY1)
        pkey.sign(ensure_bytes('test'), get_digest_algorithm())
        with pytest.raises(AttributeError):
            pkey.sign(ensure_bytes('test'), get_digest_algorithm('unknown'))
