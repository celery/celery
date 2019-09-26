# -*- coding: utf-8 -*-
"""X.509 certificates."""
from __future__ import absolute_import, unicode_literals

import datetime
import glob
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.x509 import load_pem_x509_certificate
from kombu.utils.encoding import bytes_to_str, ensure_bytes

from celery.exceptions import SecurityError
from celery.five import values

from .utils import reraise_errors

__all__ = ('Certificate', 'CertStore', 'FSCertStore')


class Certificate(object):
    """X.509 certificate."""

    def __init__(self, cert):
        with reraise_errors(
            'Invalid certificate: {0!r}', errors=(ValueError,)
        ):
            self._cert = load_pem_x509_certificate(
                ensure_bytes(cert), backend=default_backend())

    def has_expired(self):
        """Check if the certificate has expired."""
        return datetime.datetime.now() > self._cert.not_valid_after

    def get_pubkey(self):
        """Get public key from certificate."""
        return self._cert.public_key()

    def get_serial_number(self):
        """Return the serial number in the certificate."""
        return self._cert.serial_number

    def get_issuer(self):
        """Return issuer (CA) as a string."""
        return ' '.join(x.value for x in self._cert.issuer)

    def get_id(self):
        """Serial number/issuer pair uniquely identifies a certificate."""
        return '{0} {1}'.format(self.get_issuer(), self.get_serial_number())

    def verify(self, data, signature, digest):
        """Verify signature for string containing data."""
        with reraise_errors('Bad signature: {0!r}'):

            padd = padding.PSS(
                mgf=padding.MGF1(digest),
                salt_length=padding.PSS.MAX_LENGTH)

            self.get_pubkey().verify(signature,
                                     ensure_bytes(data), padd, digest)


class CertStore(object):
    """Base class for certificate stores."""

    def __init__(self):
        self._certs = {}

    def itercerts(self):
        """Return certificate iterator."""
        for c in values(self._certs):
            yield c

    def __getitem__(self, id):
        """Get certificate by id."""
        try:
            return self._certs[bytes_to_str(id)]
        except KeyError:
            raise SecurityError('Unknown certificate: {0!r}'.format(id))

    def add_cert(self, cert):
        cert_id = bytes_to_str(cert.get_id())
        if cert_id in self._certs:
            raise SecurityError('Duplicate certificate: {0!r}'.format(id))
        self._certs[cert_id] = cert


class FSCertStore(CertStore):
    """File system certificate store."""

    def __init__(self, path):
        CertStore.__init__(self)
        if os.path.isdir(path):
            path = os.path.join(path, '*')
        for p in glob.glob(path):
            with open(p) as f:
                cert = Certificate(f.read())
                if cert.has_expired():
                    raise SecurityError(
                        'Expired certificate: {0!r}'.format(cert.get_id()))
                self.add_cert(cert)
