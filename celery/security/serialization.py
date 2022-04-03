"""Secure serializer."""
from typing import TYPE_CHECKING, Any, Literal, Optional, TypedDict, Union

from kombu.serialization import dumps, loads, registry
from kombu.utils.encoding import bytes_to_str, ensure_bytes, str_to_bytes

from celery.app.defaults import DEFAULT_SECURITY_DIGEST
from celery.utils.serialization import b64decode, b64encode

from .certificate import Certificate, FSCertStore
from .key import PrivateKey
from .utils import get_digest_algorithm, reraise_errors

if TYPE_CHECKING:
    _Serializer = Literal["json", "msgpack", "yaml", "pickle"]

    class _UnpackReturn(TypedDict):
        signer: bytes
        signature: bytes
        content_type: str
        content_encoding: str
        body: str


__all__ = ('SecureSerializer', 'register_auth')


class SecureSerializer:
    """Signed serializer."""

    def __init__(self, key: Optional[PrivateKey] = None, cert: Optional[Certificate] = None,
                 cert_store: Optional[FSCertStore] = None, digest: str = DEFAULT_SECURITY_DIGEST,
                 serializer: "_Serializer" = 'json'):
        self._key = key
        self._cert = cert
        self._cert_store = cert_store
        self._digest = get_digest_algorithm(digest)
        self._serializer = serializer

    def serialize(self, data: Union[bytes, str]) -> str:
        """Serialize data structure into string."""
        assert self._key is not None
        assert self._cert is not None
        with reraise_errors('Unable to serialize: {0!r}', (Exception,)):
            content_type, content_encoding, body = dumps(
                bytes_to_str(data), serializer=self._serializer)
            # What we sign is the serialized body, not the body itself.
            # this way the receiver doesn't have to decode the contents
            # to verify the signature (and thus avoiding potential flaws
            # in the decoding step).
            body = ensure_bytes(body)
            return self._pack(body, content_type, content_encoding,
                              signature=self._key.sign(body, self._digest),
                              signer=self._cert.get_id())

    def deserialize(self, data: Union[bytes, str]) -> Any:
        """Deserialize data structure from string."""
        assert self._cert_store is not None
        with reraise_errors('Unable to deserialize: {0!r}', (Exception,)):
            payload = self._unpack(data)
            signature, signer, body = (payload['signature'],
                                       payload['signer'],
                                       payload['body'])
            self._cert_store[signer].verify(body, signature, self._digest)
        return loads(bytes_to_str(body), payload['content_type'],
                     payload['content_encoding'], force=True)

    def _pack(self, body: bytes, content_type: str, content_encoding: str,
              signer: Union[bytes, str], signature: Union[bytes, str],
              sep: bytes = str_to_bytes('\x00\x01')) -> str:
        fields = sep.join(
            ensure_bytes(s) for s in [signer, signature, content_type,
                                      content_encoding, body]
        )
        return b64encode(fields)

    def _unpack(self, payload: Union[bytes, str], sep: bytes = str_to_bytes('\x00\x01')) -> "_UnpackReturn":
        raw_payload = b64decode(ensure_bytes(payload))
        first_sep = raw_payload.find(sep)

        signer = raw_payload[:first_sep]
        signer_cert = self._cert_store[signer]

        # shift 3 bits right to get signature length
        # 2048bit rsa key has a signature length of 256
        # 4096bit rsa key has a signature length of 512
        sig_len = signer_cert.get_pubkey().key_size >> 3
        sep_len = len(sep)
        signature_start_position = first_sep + sep_len
        signature_end_position = signature_start_position + sig_len
        signature = raw_payload[
            signature_start_position:signature_end_position
        ]

        v = raw_payload[signature_end_position + sep_len:].split(sep)

        return {
            'signer': signer,
            'signature': signature,
            'content_type': bytes_to_str(v[0]),
            'content_encoding': bytes_to_str(v[1]),
            'body': bytes_to_str(v[2]),
        }


def register_auth(key: Optional[str] = None, key_password: Optional[str] = None,
                  cert: Optional[str] = None, store: Optional[str] = None, digest: str = DEFAULT_SECURITY_DIGEST,
                  serializer: "_Serializer" = 'json') -> None:
    """Register security serializer."""
    s = SecureSerializer(key and PrivateKey(key, password=key_password),
                         cert and Certificate(cert),
                         store and FSCertStore(store),
                         digest, serializer=serializer)
    registry.register('auth', s.serialize, s.deserialize,
                      content_type='application/data',
                      content_encoding='utf-8')
