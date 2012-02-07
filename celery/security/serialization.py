from __future__ import absolute_import

import base64
import sys

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
            return self._pack(body, content_type, content_encoding,
                              signature=self._key.sign(body, self._digest),
                              signer=self._cert.get_id())
        except Exception, exc:
            raise SecurityError, SecurityError(
                    "Unable to serialize: %r" % (exc, )), sys.exc_info()[2]

    def deserialize(self, data):
        """deserialize data structure from string"""
        assert self._cert_store is not None
        try:
            payload = self._unpack(data)
            signature, signer, body = (payload["signature"],
                                       payload["signer"],
                                       payload["body"])
            self._cert_store[signer].verify(body,
                                            signature, self._digest)
        except Exception, exc:
            raise SecurityError, SecurityError(
                    "Unable to deserialize: %r" % (exc, )), sys.exc_info()[2]

        return decode(body, payload["content_type"],
                            payload["content_encoding"], force=True)

    def _pack(self, body, content_type, content_encoding, signer, signature,
            sep='\x00\x01'):
        return b64encode(sep.join([signer, signature,
                                   content_type, content_encoding, body]))

    def _unpack(self, payload, sep='\x00\x01',
            fields=("signer", "signature", "content_type",
                    "content_encoding", "body")):
        return dict(zip(fields, b64decode(payload).split(sep)))


def register_auth(key=None, cert=None, store=None, digest="sha1",
        serializer="json"):
    """register security serializer"""
    s = SecureSerializer(key and PrivateKey(key),
                         cert and Certificate(cert),
                         store and FSCertStore(store),
                         digest=digest, serializer=serializer)
    registry.register("auth", s.serialize, s.deserialize,
                      content_type="application/data",
                      content_encoding="utf-8")
