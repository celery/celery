from __future__ import absolute_import

from celery.exceptions import SecurityError
from celery.security.certificate import Certificate, CertStore, FSCertStore

from mock import Mock, patch

from . import CERT1, CERT2, KEY1
from .case import SecurityCase


class test_Certificate(SecurityCase):

    def test_valid_certificate(self):
        Certificate(CERT1)
        Certificate(CERT2)

    def test_invalid_certificate(self):
        self.assertRaises(TypeError, Certificate, None)
        self.assertRaises(SecurityError, Certificate, "")
        self.assertRaises(SecurityError, Certificate, "foo")
        self.assertRaises(SecurityError, Certificate, CERT1[:20] + CERT1[21:])
        self.assertRaises(SecurityError, Certificate, KEY1)

    def test_has_expired(self):
        self.assertFalse(Certificate(CERT1).has_expired())


class test_CertStore(SecurityCase):

    def test_itercerts(self):
        cert1 = Certificate(CERT1)
        cert2 = Certificate(CERT2)
        certstore = CertStore()
        for c in certstore.itercerts():
            self.assertTrue(False)
        certstore.add_cert(cert1)
        certstore.add_cert(cert2)
        for c in certstore.itercerts():
            self.assertIn(c, (cert1, cert2))

    def test_duplicate(self):
        cert1 = Certificate(CERT1)
        certstore = CertStore()
        certstore.add_cert(cert1)
        self.assertRaises(SecurityError, certstore.add_cert, cert1)


class test_FSCertStore(SecurityCase):

    @patch("os.path.isdir")
    @patch("glob.glob")
    @patch("celery.security.certificate.Certificate")
    @patch("__builtin__.open")
    def test_init(self, open_, Certificate, glob, isdir):
        cert = Certificate.return_value = Mock()
        cert.has_expired.return_value = False
        isdir.return_value = True
        glob.return_value = ["foo.cert"]
        op = open_.return_value = Mock()
        op.__enter__ = Mock()
        def on_exit(*x):
            if x[0]:
                print(x)
                raise x[0], x[1], x[2]
        op.__exit__ = Mock()
        op.__exit__.side_effect = on_exit
        cert.get_id.return_value = 1
        x = FSCertStore("/var/certs")
        self.assertIn(1, x._certs)
        glob.assert_called_with("/var/certs/*")
        op.__enter__.assert_called_with()
        op.__exit__.assert_called_with(None, None, None)

        # they both end up with the same id
        glob.return_value = ["foo.cert", "bar.cert"]
        with self.assertRaises(SecurityError):
            x = FSCertStore("/var/certs")
        glob.return_value = ["foo.cert"]

        cert.has_expired.return_value = True
        with self.assertRaises(SecurityError):
            x = FSCertStore("/var/certs")

        isdir.return_value = False
        with self.assertRaises(SecurityError):
            x = FSCertStore("/var/certs")
