import os
import glob

try:
    from OpenSSL import crypto
except ImportError:
    crypto = None

from celery.security.exceptions import SecurityError

class Certificate(object):
    """X.509 certificate"""
    def __init__(self, cert):
        assert crypto is not None
        try:
            self._cert = crypto.load_certificate(crypto.FILETYPE_PEM, cert)
        except crypto.Error, e:
            raise SecurityError("Invalid certificate", e)

    def has_expired(self):
        """check if the certificate has expired"""
        return self._cert.has_expired()

    def get_serial_number(self):
        """return certificate serial number"""
        return self._cert.get_serial_number()

    def get_issuer(self):
        """return issuer (CA) as a string"""
        return ' '.join(map(lambda x: x[1],
                            self._cert.get_issuer().get_components()))

    def get_id(self):
        """serial number/issuer pair uniquely identifies a certificate"""
        return "%s %s" % (self.get_issuer(), self.get_serial_number())

    def verify(self, data, signature):
        """verify the signature for a data string"""
        try:
            crypto.verify(self._cert, signature, data, 'sha1')
        except crypto.Error, e:
            raise SecurityError("Bad signature", e)

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
        except KeyError, e:
            raise SecurityError("Unknown certificate: %s" % id, e)

    def add_cert(self, cert):
        if cert.get_id() in self._certs:
            raise SecurityError("Duplicate certificate: %s" % id)
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
                    raise SecurityError("Expired certificate: %s" %\
                                        cert.get_id())
                self.add_cert(cert)

