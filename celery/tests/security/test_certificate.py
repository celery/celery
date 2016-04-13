from __future__ import absolute_import, unicode_literals

from celery.exceptions import SecurityError
from celery.security.certificate import Certificate, CertStore, FSCertStore

from . import CERT1, CERT2, KEY1
from .case import SecurityCase

from celery.tests.case import Mock, mock, patch, skip


class test_Certificate(SecurityCase):

    def test_valid_certificate(self):
        Certificate(CERT1)
        Certificate(CERT2)

    def test_invalid_certificate(self):
        with self.assertRaises((SecurityError, TypeError)):
            Certificate(None)
        with self.assertRaises(SecurityError):
            Certificate('')
        with self.assertRaises(SecurityError):
            Certificate('foo')
        with self.assertRaises(SecurityError):
            Certificate(CERT1[:20] + CERT1[21:])
        with self.assertRaises(SecurityError):
            Certificate(KEY1)

    @skip.todo(reason='cert expired')
    def test_has_expired(self):
        self.assertFalse(Certificate(CERT1).has_expired())

    def test_has_expired_mock(self):
        x = Certificate(CERT1)
        x._cert = Mock(name='cert')
        self.assertIs(x.has_expired(), x._cert.has_expired())


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
        with self.assertRaises(SecurityError):
            certstore.add_cert(cert1)


class test_FSCertStore(SecurityCase):

    @patch('os.path.isdir')
    @patch('glob.glob')
    @patch('celery.security.certificate.Certificate')
    def test_init(self, Certificate, glob, isdir):
        cert = Certificate.return_value = Mock()
        cert.has_expired.return_value = False
        isdir.return_value = True
        glob.return_value = ['foo.cert']
        with mock.open():
            cert.get_id.return_value = 1
            x = FSCertStore('/var/certs')
            self.assertIn(1, x._certs)
            glob.assert_called_with('/var/certs/*')

            # they both end up with the same id
            glob.return_value = ['foo.cert', 'bar.cert']
            with self.assertRaises(SecurityError):
                x = FSCertStore('/var/certs')
            glob.return_value = ['foo.cert']

            cert.has_expired.return_value = True
            with self.assertRaises(SecurityError):
                x = FSCertStore('/var/certs')

            isdir.return_value = False
            with self.assertRaises(SecurityError):
                x = FSCertStore('/var/certs')
