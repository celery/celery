"""Private keys for the security serializer."""
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from kombu.utils.encoding import ensure_bytes

from .utils import reraise_errors

try:
    from cryptography.hazmat.primitives.asymmetric import mldsa
    MLDSA_PRIVATE_KEY_TYPES = (
        mldsa.MLDSA44PrivateKey,
        mldsa.MLDSA65PrivateKey,
        mldsa.MLDSA87PrivateKey,
    )
    _HAS_MLDSA = True
except ImportError:
    MLDSA_PRIVATE_KEY_TYPES = ()
    _HAS_MLDSA = False

__all__ = ('PrivateKey',)

_SUPPORTED_KEY_TYPES = (rsa.RSAPrivateKey,) + MLDSA_PRIVATE_KEY_TYPES


class PrivateKey:
    """Represents a private key."""

    def __init__(self, key, password=None):
        with reraise_errors(
            'Invalid private key: {0!r}', errors=(ValueError,)
        ):
            self._key = serialization.load_pem_private_key(
                ensure_bytes(key),
                password=ensure_bytes(password),
                backend=default_backend())

            if not isinstance(self._key, _SUPPORTED_KEY_TYPES):
                raise ValueError(
                    "Unsupported key type. "
                    "Only RSA and ML-DSA keys are supported."
                )

    def _is_mldsa(self):
        """Return True if the underlying key is an ML-DSA key."""
        return _HAS_MLDSA and isinstance(self._key, MLDSA_PRIVATE_KEY_TYPES)

    def sign(self, data, digest):
        """Sign string containing data."""
        with reraise_errors('Unable to sign data: {0!r}'):

            pad = padding.PSS(
                mgf=padding.MGF1(digest),
                salt_length=padding.PSS.MAX_LENGTH)

            return self._key.sign(ensure_bytes(data), pad, digest)
