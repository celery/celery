from __future__ import absolute_import

from celery.exceptions import SecurityError
from celery.security.certificate import Certificate, CertStore

from . import CERT1, CERT2, KEY1
from .case import SecurityCase


class TestCertificate(SecurityCase):

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


class TestCertStore(SecurityCase):

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
