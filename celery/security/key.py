# -*- coding: utf-8 -*-
"""
    celery.security.key
    ~~~~~~~~~~~~~~~~~~~

    Private key for the security serializer.

"""
from __future__ import absolute_import
from __future__ import with_statement

from .utils import crypto, reraise_errors


class PrivateKey(object):

    def __init__(self, key):
        with reraise_errors('Invalid private key: %r'):
            self._key = crypto.load_privatekey(crypto.FILETYPE_PEM, key)

    def sign(self, data, digest):
        """sign string containing data."""
        with reraise_errors('Unable to sign data: %r'):
            return crypto.sign(self._key, data, digest)
