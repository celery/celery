from celery.tests.utils import unittest

from celery.security.key import PrivateKey
from celery.security.exceptions import SecurityError
from celery.tests.test_security import CERT1, CERT2, KEY1, KEY2

class TestKey(unittest.TestCase):

    def test_valid_private_key(self):
        PrivateKey(KEY1)
        PrivateKey(KEY2)

    def test_invalid_private_key(self):
        self.assertRaises(TypeError, PrivateKey, None)
        self.assertRaises(SecurityError, PrivateKey, "")
        self.assertRaises(SecurityError, PrivateKey, "foo")
        self.assertRaises(SecurityError, PrivateKey, KEY1[:20] + KEY1[21:])
        self.assertRaises(SecurityError, PrivateKey, CERT1)

