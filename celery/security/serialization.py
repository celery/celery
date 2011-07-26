import anyjson

from kombu.serialization import registry

from celery.security.certificate import Certificate, FSCertStore
from celery.security.key import PrivateKey
from celery.security.exceptions import SecurityError

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
            signature = self._key.sign(data).encode("base64")
            signer = self._cert.get_id()
            return self._serialize(dict(data=data,
                                        signer=signer,
                                        signature=signature))
        except Exception, e:
            raise SecurityError("Unable to serialize", e)

    def deserialize(self, data):
        """deserialize data structure from string"""
        assert self._cert_store is not None
        try:
            data = self._deserialize(data)
            signature = data['signature'].decode("base64")
            signer = data['signer']
            data = data['data']
            self._cert_store[signer].verify(data, signature)
            return self._deserialize(data)
        except Exception, e:
            raise SecurityError("Unable to deserialize", e)

def register_auth(key=None, cert=None, store=None):
    """register security serializer"""
    global s
    s = SecureSerializer(key and PrivateKey(key),
                         cert and Certificate(cert),
                         store and FSCertStore(store),
                         anyjson.serialize, anyjson.deserialize)
    registry.register("auth", s.serialize, s.deserialize,
                      content_type='application/data',
                      content_encoding='utf-8')

