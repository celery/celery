import pickle

from kombu.serialization import registry

from celery.security.certificate import Certificate, FSCertStore
from celery.security.key import PrivateKey
from celery.security.exceptions import SecurityError

class SecureSerializer(object):

    def __init__(self, key=None, cert=None, cert_store=None):
        self._key = key
        self._cert = cert
        self._cert_store = cert_store

    def serialize(self, data):
        """serialize data structure into string"""
        assert self._key is not None
        assert self._cert is not None
        try:
            data = pickle.dumps(data)
            signature = self._key.sign(data)
            signer = self._cert.get_id()
            return pickle.dumps(dict(data=data,
                                     signer=signer,
                                     signature=signature))
        except Exception, e:
            raise SecurityError("Unable to serialize", e)

    def deserialize(self, data):
        """deserialize data structure from string"""
        assert self._cert_store is not None
        try:
            data = pickle.loads(data)
            signature = data['signature']
            signer = data['signer']
            data = data['data']
            self._cert_store[signer].verify(data, signature)
            return pickle.loads(data)
        except Exception, e:
            raise SecurityError("Unable to deserialize", e)

def register_auth(key=None, cert=None, store=None):
    """register security serializer"""
    global s
    s = SecureSerializer(key and PrivateKey(key),
                         cert and Certificate(cert),
                         store and FSCertStore(store))
    registry.register("auth", s.serialize, s.deserialize,
                      content_type='application/data',
                      content_encoding='binary')

