# -*- coding: utf-8 -*-
"""
    celery.security.certificate
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    X.509 certificates.

"""
from __future__ import absolute_import
from __future__ import with_statement

import glob
import os

from celery.exceptions import SecurityError

from .utils import crypto, reraise_errors


class Certificate(object):
    """X.509 certificate."""

    def __init__(self, cert):
        assert crypto is not None
        with reraise_errors('Invalid certificate: %r'):
            self._cert = crypto.load_certificate(crypto.FILETYPE_PEM, cert)

    def has_expired(self):
        """Check if the certificate has expired."""
        return self._cert.has_expired()

    def get_serial_number(self):
        """Returns the certificates serial number."""
        return self._cert.get_serial_number()

    def get_issuer(self):
        """Returns issuer (CA) as a string"""
        return ' '.join(x[1] for x in
                        self._cert.get_issuer().get_components())

    def get_id(self):
        """Serial number/issuer pair uniquely identifies a certificate"""
        return '%s %s' % (self.get_issuer(), self.get_serial_number())

    def verify(self, data, signature, digest):
        """Verifies the signature for string containing data."""
        with reraise_errors('Bad signature: %r'):
            crypto.verify(self._cert, signature, data, digest)


class CertStore(object):
    """Base class for certificate stores"""

    def __init__(self):
        self._certs = {}

    def itercerts(self):
        """an iterator over the certificates"""
        for c in self._certs.itervalues():
            yield c

    def __getitem__(self, id):
        """get certificate by id"""
        try:
            return self._certs[id]
        except KeyError:
            raise SecurityError('Unknown certificate: %r' % (id, ))

    def add_cert(self, cert):
        if cert.get_id() in self._certs:
            raise SecurityError('Duplicate certificate: %r' % (id, ))
        self._certs[cert.get_id()] = cert


class FSCertStore(CertStore):
    """File system certificate store"""

    def __init__(self, path):
        CertStore.__init__(self)
        if os.path.isdir(path):
            path = os.path.join(path, '*')
        for p in glob.glob(path):
            with open(p) as f:
                cert = Certificate(f.read())
                if cert.has_expired():
                    raise SecurityError(
                        'Expired certificate: %r' % (cert.get_id(), ))
                self.add_cert(cert)
