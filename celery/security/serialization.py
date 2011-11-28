from __future__ import absolute_import

import anyjson
import base64

from kombu.serialization import registry, encode, decode

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
            digest="sha1", serializer="json"):
        self._key = key
        self._cert = cert
        self._cert_store = cert_store
        self._digest = digest
        self._serialize = anyjson.serialize
        self._deserialize = anyjson.deserialize
        self._serializer = serializer

    def serialize(self, data):
        """serialize data structure into string"""
        assert self._key is not None
        assert self._cert is not None
        try:
            content_type, content_encoding, body = encode(
                    data, serializer=self._serializer)
            # What we sign is the serialized body, not the body itself.
            # this way the receiver doesn't have to decode the contents
            # to verify the signature (and thus avoiding potential flaws
            # in the decoding step).
            signature = b64encode(self._key.sign(body, self._digest))
            signer = self._cert.get_id()
            return self._serialize({
                    "body": body,
                    "signer": self._cert.get_id(),
                    "signature": signature,
                    "content_type": content_type,
                    "content_encoding": content_encoding,
            })
        except Exception, exc:
            raise SecurityError("Unable to serialize: %r" % (exc, ))

    def deserialize(self, data):
        """deserialize data structure from string"""
        assert self._cert_store is not None
        try:
            data = self._deserialize(data)
            signature = b64decode(data["signature"])
            signer = data["signer"]
            body = data["body"]
            self._cert_store[signer].verify(body,
                                            signature, self._digest)
            return decode(body, data["content_type"],
                                data["content_encoding"], force=True)
        except Exception, exc:
            raise SecurityError("Unable to deserialize: %r" % (exc, ))


def register_auth(key=None, cert=None, store=None, digest="sha1",
        serializer="pickle"):
    """register security serializer"""
    s = SecureSerializer(key and PrivateKey(key),
                         cert and Certificate(cert),
                         store and FSCertStore(store),
                         digest=digest, serializer=serializer)
    registry.register("auth", s.serialize, s.deserialize,
                      content_type="application/data",
                      content_encoding="utf-8")
