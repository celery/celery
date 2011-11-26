from __future__ import absolute_import

import anyjson
import base64

from kombu.serialization import registry

from ..exceptions import SecurityError
from ..utils.encoding import bytes_to_str, str_to_bytes

from .certificate import Certificate, FSCertStore
from .key import PrivateKey


def b64encode(s):
    return bytes_to_str(base64.b64encode(str_to_bytes(s)))


def b64decode(s):
    return base64.b64decode(str_to_bytes(s))


class SecureSerializer(object):

    def __init__(self, key=None, cert=None, cert_store=None,
            serialize=anyjson.serialize,
            deserialize=anyjson.deserialize):
        self._key = key
        self._cert = cert
        self._cert_store = cert_store
        self._serialize = serialize
        self._deserialize = deserialize

    def serialize(self, data):
        """serialize data structure into string"""
        assert self._key is not None
        assert self._cert is not None
        try:
            data = self._serialize(data)
            signature = b64encode(self._key.sign(data))
            signer = self._cert.get_id()
            return self._serialize(dict(data=data,
                                        signer=signer,
                                        signature=signature))
        except Exception, exc:
            raise SecurityError("Unable to serialize: %r" % (exc, ))

    def deserialize(self, data):
        """deserialize data structure from string"""
        assert self._cert_store is not None
        try:
            data = self._deserialize(data)
            signature = b64decode(data["signature"])
            signer = data["signer"]
            data = data["data"]
            self._cert_store[signer].verify(data, signature)
            return self._deserialize(data)
        except Exception, exc:
            raise SecurityError("Unable to deserialize: %r" % (exc, ))


def register_auth(key=None, cert=None, store=None):
    """register security serializer"""
    s = SecureSerializer(key and PrivateKey(key),
                         cert and Certificate(cert),
                         store and FSCertStore(store),
                         anyjson.serialize, anyjson.deserialize)
    registry.register("auth", s.serialize, s.deserialize,
                      content_type="application/data",
                      content_encoding="utf-8")
