from __future__ import absolute_import

import sys

try:
    from OpenSSL import crypto
except ImportError:
    crypto = None  # noqa

from ..exceptions import SecurityError


class PrivateKey(object):

    def __init__(self, key):
        assert crypto is not None
        try:
            self._key = crypto.load_privatekey(crypto.FILETYPE_PEM, key)
        except crypto.Error, exc:
            raise SecurityError, SecurityError(
                    "Invalid private key: %r" % (exc, )), sys.exc_info()[2]

    def sign(self, data, digest):
        """sign string containing data."""
        try:
            return crypto.sign(self._key, data, digest)
        except crypto.Error, exc:
            raise SecurityError, SecurityError(
                    "Unable to sign data: %r" % (exc, )), sys.exc_info()[2]
