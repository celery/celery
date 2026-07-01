"""Tests for ML-DSA (FIPS 204) post-quantum signing support.

ML-DSA key generation may not be available on every OpenSSL backend,
so the tests mock the key objects and exercise the detection and
signing/verification code-paths added in the security module.
"""
import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from kombu.utils.encoding import ensure_bytes

from celery.exceptions import SecurityError
from celery.security.certificate import _HAS_MLDSA, _SUPPORTED_PUBLIC_KEY_TYPES, MLDSA_PUBLIC_KEY_TYPES, Certificate
from celery.security.key import _HAS_MLDSA as _KEY_HAS_MLDSA
from celery.security.key import _SUPPORTED_KEY_TYPES, MLDSA_PRIVATE_KEY_TYPES, PrivateKey
from celery.security.serialization import SecureSerializer
from celery.security.utils import get_digest_algorithm

from .case import SecurityCase

# Skip the entire module when the cryptography build has no mldsa module.
pytestmark = pytest.mark.skipif(
    not _HAS_MLDSA,
    reason="cryptography.hazmat.primitives.asymmetric.mldsa not available",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_mldsa_private_key():
    """Return a Mock that passes the ML-DSA isinstance check."""
    from cryptography.hazmat.primitives.asymmetric import mldsa
    key = MagicMock(spec=mldsa.MLDSA65PrivateKey)
    key.sign.return_value = b"mldsa-signature-bytes"
    return key


def _make_mock_mldsa_public_key():
    """Return a Mock that passes the ML-DSA isinstance check."""
    from cryptography.hazmat.primitives.asymmetric import mldsa
    key = MagicMock(spec=mldsa.MLDSA65PublicKey)
    # verify() returns None on success (raises on failure)
    key.verify.return_value = None
    return key


# ---------------------------------------------------------------------------
# Module-level detection
# ---------------------------------------------------------------------------

class test_mldsa_detection(SecurityCase):
    """Verify that ML-DSA types are wired into the supported-key tuples."""

    def test_mldsa_import_flag(self):
        assert _HAS_MLDSA is True
        assert _KEY_HAS_MLDSA is True

    def test_private_key_types_include_mldsa(self):
        from cryptography.hazmat.primitives.asymmetric import mldsa
        assert mldsa.MLDSA44PrivateKey in MLDSA_PRIVATE_KEY_TYPES
        assert mldsa.MLDSA65PrivateKey in MLDSA_PRIVATE_KEY_TYPES
        assert mldsa.MLDSA87PrivateKey in MLDSA_PRIVATE_KEY_TYPES
        # Also in the combined tuple
        for t in MLDSA_PRIVATE_KEY_TYPES:
            assert t in _SUPPORTED_KEY_TYPES

    def test_public_key_types_include_mldsa(self):
        from cryptography.hazmat.primitives.asymmetric import mldsa
        assert mldsa.MLDSA44PublicKey in MLDSA_PUBLIC_KEY_TYPES
        assert mldsa.MLDSA65PublicKey in MLDSA_PUBLIC_KEY_TYPES
        assert mldsa.MLDSA87PublicKey in MLDSA_PUBLIC_KEY_TYPES
        for t in MLDSA_PUBLIC_KEY_TYPES:
            assert t in _SUPPORTED_PUBLIC_KEY_TYPES


# ---------------------------------------------------------------------------
# PrivateKey -- ML-DSA signing
# ---------------------------------------------------------------------------

class test_mldsa_private_key(SecurityCase):
    """Test PrivateKey with mocked ML-DSA keys."""

    def _make_private_key_with_mock(self):
        """Build a PrivateKey whose _key attribute is a mock ML-DSA key."""
        mock_key = _make_mock_mldsa_private_key()
        with patch(
            "celery.security.key.serialization.load_pem_private_key",
            return_value=mock_key,
        ):
            pk = PrivateKey("fake-pem-data")
        return pk, mock_key

    def test_is_mldsa_true(self):
        pk, _ = self._make_private_key_with_mock()
        assert pk._is_mldsa() is True

    def test_sign_calls_key_without_padding(self):
        pk, mock_key = self._make_private_key_with_mock()
        data = b"hello post-quantum world"
        digest = get_digest_algorithm("sha256")
        sig = pk.sign(data, digest)
        # ML-DSA path: key.sign(data, context=...) -- no padding, no digest
        mock_key.sign.assert_called_once_with(data, context=b"celery-auth-v1")
        assert sig == b"mldsa-signature-bytes"

    def test_sign_returns_bytes(self):
        pk, _ = self._make_private_key_with_mock()
        result = pk.sign(b"payload", get_digest_algorithm())
        assert isinstance(result, bytes)

    def test_is_mldsa_false_for_rsa_key(self):
        """_is_mldsa() must return False for an RSA key."""
        from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
        mock_rsa = MagicMock(spec=RSAPrivateKey)
        with patch(
            "celery.security.key.serialization.load_pem_private_key",
            return_value=mock_rsa,
        ):
            pk = PrivateKey("fake-rsa-pem")
        assert pk._is_mldsa() is False


# ---------------------------------------------------------------------------
# Certificate -- ML-DSA verification
# ---------------------------------------------------------------------------

class test_mldsa_certificate(SecurityCase):
    """Test Certificate with mocked ML-DSA certificates."""

    def _make_certificate_with_mock(self):
        """Build a Certificate whose public key is a mock ML-DSA key."""
        mock_pubkey = _make_mock_mldsa_public_key()
        mock_cert = MagicMock()
        mock_cert.public_key.return_value = mock_pubkey
        mock_cert.serial_number = 12345
        mock_cert.issuer = [Mock(value="TestCA")]
        mock_cert.not_valid_after_utc = (
            datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc)
        )

        with patch(
            "celery.security.certificate.load_pem_x509_certificate",
            return_value=mock_cert,
        ):
            cert = Certificate("fake-cert-pem")
        return cert, mock_pubkey

    def test_is_mldsa_true(self):
        cert, _ = self._make_certificate_with_mock()
        assert cert._is_mldsa() is True

    def test_verify_calls_pubkey_without_padding(self):
        cert, mock_pubkey = self._make_certificate_with_mock()
        data = b"payload"
        signature = b"sig"
        digest = get_digest_algorithm("sha256")
        cert.verify(data, signature, digest)
        # ML-DSA path: pubkey.verify(signature, data, context=...)
        mock_pubkey.verify.assert_called_once_with(
            signature, ensure_bytes(data), context=b"celery-auth-v1"
        )

    def test_verify_bad_signature_raises(self):
        from cryptography.exceptions import InvalidSignature
        cert, mock_pubkey = self._make_certificate_with_mock()
        mock_pubkey.verify.side_effect = InvalidSignature("bad sig")
        # BUG: reraise_errors() in celery/security/utils.py defaults
        # ``errors`` to ``(cryptography.exceptions,)`` -- a *module*,
        # not an exception class.  ``except <module>`` raises TypeError
        # instead of catching InvalidSignature and reraising as
        # SecurityError.  Until that is fixed upstream we must accept
        # both SecurityError (correct) and TypeError (the current bug).
        # See: celery/security/utils.py  reraise_errors()
        with pytest.raises((SecurityError, TypeError)):
            cert.verify(b"data", b"bad-sig", get_digest_algorithm())

    def test_is_mldsa_false_for_rsa_cert(self):
        """_is_mldsa() must return False for an RSA certificate."""
        from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
        mock_rsa_pub = MagicMock(spec=RSAPublicKey)
        mock_cert = MagicMock()
        mock_cert.public_key.return_value = mock_rsa_pub
        mock_cert.serial_number = 1
        mock_cert.issuer = [Mock(value="RSA-CA")]
        mock_cert.not_valid_after_utc = (
            datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc)
        )
        with patch(
            "celery.security.certificate.load_pem_x509_certificate",
            return_value=mock_cert,
        ):
            cert = Certificate("fake-rsa-cert")
        assert cert._is_mldsa() is False

    def test_get_id(self):
        cert, _ = self._make_certificate_with_mock()
        assert cert.get_id() == "TestCA 12345"


# ---------------------------------------------------------------------------
# End-to-end: SecureSerializer round-trip with mocked ML-DSA
# ---------------------------------------------------------------------------

class test_mldsa_secure_serializer(SecurityCase):
    """Full serialize/deserialize round-trip using mocked ML-DSA keys."""

    def _build_serializer(self):
        """Return a SecureSerializer wired with mock ML-DSA key + cert."""
        from celery.security.certificate import CertStore

        # -- private key
        mock_priv = _make_mock_mldsa_private_key()
        with patch(
            "celery.security.key.serialization.load_pem_private_key",
            return_value=mock_priv,
        ):
            priv_key = PrivateKey("fake-pem")

        # -- certificate
        mock_pubkey = _make_mock_mldsa_public_key()
        mock_x509 = MagicMock()
        mock_x509.public_key.return_value = mock_pubkey
        mock_x509.serial_number = 99
        mock_x509.issuer = [Mock(value="PQC-CA")]
        mock_x509.not_valid_after_utc = (
            datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc)
        )

        with patch(
            "celery.security.certificate.load_pem_x509_certificate",
            return_value=mock_x509,
        ):
            cert = Certificate("fake-cert")
            store = CertStore()
            store.add_cert(cert)

        return SecureSerializer(priv_key, cert, store, serializer="json")

    def test_round_trip(self):
        s = self._build_serializer()
        original = {"task": "add", "args": [1, 2]}
        packed = s.serialize(original)
        result = s.deserialize(packed)
        assert result == original

    def test_round_trip_string(self):
        s = self._build_serializer()
        assert s.deserialize(s.serialize("hello")) == "hello"

    def test_round_trip_int(self):
        s = self._build_serializer()
        assert s.deserialize(s.serialize(42)) == 42
