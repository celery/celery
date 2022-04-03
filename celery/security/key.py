"""Private keys for the security serializer."""
from ctypes import Union
from typing import TYPE_CHECKING, Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from kombu.utils.encoding import ensure_bytes

from .utils import reraise_errors

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.utils import Prehashed
    from cryptography.hazmat.primitives.hashes import HashAlgorithm


__all__ = ('PrivateKey',)


class PrivateKey:
    """Represents a private key."""

    def __init__(self, key: str, password: Optional[str] = None) -> None:
        with reraise_errors(
            'Invalid private key: {0!r}', errors=(ValueError,)
        ):
            self._key = serialization.load_pem_private_key(
                ensure_bytes(key),
                password=ensure_bytes(password),
                backend=default_backend())

    def sign(self, data: Union[bytes, str], digest: Union["HashAlgorithm", "Prehashed"]) -> bytes:
        """Sign string containing data."""
        with reraise_errors('Unable to sign data: {0!r}'):

            padd = padding.PSS(
                mgf=padding.MGF1(digest),
                salt_length=padding.PSS.MAX_LENGTH)

            return self._key.sign(ensure_bytes(data), padd, digest)
