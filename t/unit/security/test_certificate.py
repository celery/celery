import datetime
import os

import pytest
from case import Mock, mock, patch, skip

from celery.exceptions import SecurityError
from celery.security.certificate import Certificate, CertStore, FSCertStore

from . import CERT1, CERT2, KEY1
from .case import SecurityCase


class test_Certificate(SecurityCase):

    def test_valid_certificate(self):
        Certificate(CERT1)
        Certificate(CERT2)

    def test_invalid_certificate(self):
        with pytest.raises((SecurityError, TypeError)):
            Certificate(None)
        with pytest.raises(SecurityError):
            Certificate('')
        with pytest.raises(SecurityError):
            Certificate('foo')
        with pytest.raises(SecurityError):
            Certificate(CERT1[:20] + CERT1[21:])
        with pytest.raises(SecurityError):
            Certificate(KEY1)

    @skip.todo(reason='cert expired')
    def test_has_expired(self):
        assert not Certificate(CERT1).has_expired()

    def test_has_expired_mock(self):
        x = Certificate(CERT1)

        x._cert = Mock(name='cert')
        time_after = datetime.datetime.now() + datetime.timedelta(days=-1)
        x._cert.not_valid_after = time_after

        assert x.has_expired() is True

    def test_has_not_expired_mock(self):
        x = Certificate(CERT1)

        x._cert = Mock(name='cert')
        time_after = datetime.datetime.now() + datetime.timedelta(days=1)
        x._cert.not_valid_after = time_after

        assert x.has_expired() is False


class test_CertStore(SecurityCase):

    def test_itercerts(self):
        cert1 = Certificate(CERT1)
        cert2 = Certificate(CERT2)
        certstore = CertStore()
        for c in certstore.itercerts():
            assert False
        certstore.add_cert(cert1)
        certstore.add_cert(cert2)
        for c in certstore.itercerts():
            assert c in (cert1, cert2)

    def test_duplicate(self):
        cert1 = Certificate(CERT1)
        certstore = CertStore()
        certstore.add_cert(cert1)
        with pytest.raises(SecurityError):
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

            path = os.path.join('var', 'certs')
            x = FSCertStore(path)
            assert 1 in x._certs
            glob.assert_called_with(os.path.join(path, '*'))

            # they both end up with the same id
            glob.return_value = ['foo.cert', 'bar.cert']
            with pytest.raises(SecurityError):
                x = FSCertStore(path)
            glob.return_value = ['foo.cert']

            cert.has_expired.return_value = True
            with pytest.raises(SecurityError):
                x = FSCertStore(path)

            isdir.return_value = False
            with pytest.raises(SecurityError):
                x = FSCertStore(path)
