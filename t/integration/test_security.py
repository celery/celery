from __future__ import absolute_import, unicode_literals

import datetime
import os
import tempfile

import pytest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from .tasks import add


class test_security:

    @pytest.fixture(autouse=True, scope='class')
    def class_certs(self, request):
        self.tmpdir = tempfile.mkdtemp()
        self.key_name = 'worker.key'
        self.cert_name = 'worker.pem'

        key = self.gen_private_key()
        cert = self.gen_certificate(key=key,
                                    common_name='celery cecurity integration')

        pem_key = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        )

        pem_cert = cert.public_bytes(
            encoding=serialization.Encoding.PEM,
        )

        with open(self.tmpdir + '/' + self.key_name, 'wb') as key:
            key.write(pem_key)
        with open(self.tmpdir + '/' + self.cert_name, 'wb') as cert:
            cert.write(pem_cert)

        request.cls.tmpdir = self.tmpdir
        request.cls.key_name = self.key_name
        request.cls.cert_name = self.cert_name

        yield

        os.remove(self.tmpdir + '/' + self.key_name)
        os.remove(self.tmpdir + '/' + self.cert_name)
        os.rmdir(self.tmpdir)

    @pytest.fixture(autouse=True)
    def _prepare_setup(self, manager):
        manager.app.conf.update(
            security_key='{0}/{1}'.format(self.tmpdir, self.key_name),
            security_certificate='{0}/{1}'.format(self.tmpdir, self.cert_name),
            security_cert_store='{0}/*.pem'.format(self.tmpdir),
            task_serializer='auth',
            event_serializer='auth',
            accept_content=['auth'],
            result_accept_content=['json']
        )

        manager.app.setup_security()

    def gen_private_key(self):
        """generate a private key with cryptography"""
        return rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend(),
        )

    def gen_certificate(self, key, common_name, issuer=None, sign_key=None):
        """generate a certificate with cryptography"""

        now = datetime.datetime.utcnow()

        certificate = x509.CertificateBuilder().subject_name(
            x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            ])
        ).issuer_name(
            x509.Name([
                x509.NameAttribute(
                    NameOID.COMMON_NAME,
                    issuer or common_name
                )
            ])
        ).not_valid_before(
            now
        ).not_valid_after(
            now + datetime.timedelta(seconds=86400)
        ).serial_number(
            x509.random_serial_number()
        ).public_key(
            key.public_key()
        ).add_extension(
            x509.BasicConstraints(ca=True, path_length=0), critical=True
        ).sign(
            private_key=sign_key or key,
            algorithm=hashes.SHA256(),
            backend=default_backend()
        )
        return certificate

    @pytest.mark.xfail(reason="Issue #5269")
    def test_security_task_done(self):
        t1 = add.delay(1, 1)
        assert t1.get() == 2
