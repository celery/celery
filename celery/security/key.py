try:
    from OpenSSL import crypto
except ImportError:
    crypto = None

from celery.security.exceptions import SecurityError

class PrivateKey(object):
    def __init__(self, key):
        assert crypto is not None
        try:
            self._key = crypto.load_privatekey(crypto.FILETYPE_PEM, key)
        except crypto.Error, e:
            raise SecurityError("Invalid private key", e)

    def sign(self, data):
        """sign a data string"""
        try:
            return crypto.sign(self._key, data, 'sha1')
        except crypto.Error, e:
            raise SecurityError("Unable to sign a data string", e)

