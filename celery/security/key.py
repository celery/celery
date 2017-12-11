# -*- coding: utf-8 -*-
"""Private keys for the security serializer."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from kombu.utils.encoding import ensure_bytes

from .utils import crypto, reraise_errors

__all__ = ('PrivateKey',)


class PrivateKey:
    """Represents a private key."""

    def __init__(self, key):
        with reraise_errors('Invalid private key: {0!r}'):
            self._key = crypto.load_privatekey(crypto.FILETYPE_PEM, key)

    def sign(self, data, digest):
        """Sign string containing data."""
        with reraise_errors('Unable to sign data: {0!r}'):
            return crypto.sign(self._key, ensure_bytes(data), digest)
