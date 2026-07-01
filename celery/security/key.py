"""Private keys for the security serializer."""
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from kombu.utils.encoding import ensure_bytes

from .utils import reraise_errors

MLDSA_PRIVATE_KEY_TYPES: tuple = ()
try:
    from cryptography.hazmat.primitives.asymmetric import mldsa
    MLDSA_PRIVATE_KEY_TYPES = (
        mldsa.MLDSA44PrivateKey,
        mldsa.MLDSA65PrivateKey,
        mldsa.MLDSA87PrivateKey,
    )
    _HAS_MLDSA = True
except ImportError:
    _HAS_MLDSA = False

# Domain-separation context for ML-DSA signatures.  Binding signatures to
# this tag prevents cross-protocol replay: a signature produced by Celery
# cannot be mistaken for a signature generated in a different application
# context, even when the same ML-DSA key is reused elsewhere.
_MLDSA_CONTEXT = b"celery-auth-v1"

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

            if self._is_mldsa():
                # ML-DSA uses a built-in hash internally; no digest
                # algorithm or padding scheme is required.
                return self._key.sign(ensure_bytes(data), context=_MLDSA_CONTEXT)

            pad = padding.PSS(
                mgf=padding.MGF1(digest),
                salt_length=padding.PSS.MAX_LENGTH)

            return self._key.sign(ensure_bytes(data), pad, digest)
