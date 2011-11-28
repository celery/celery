from __future__ import absolute_import
from __future__ import with_statement

import warnings

from kombu.serialization import registry, SerializerNotInstalled

from .. import current_app
from ..exceptions import ImproperlyConfigured

from .serialization import register_auth

SSL_NOT_INSTALLED = """\
You need to install the pyOpenSSL library to use the auth serializer.
Please install by:

    $ pip install pyOpenSSL
"""
AUTH_DISABLED = """\
setup_security called, but not configured to use auth serializer.
Please set CELERY_TASK_SERIALIZER="auth" to enable security.\
"""

SETTING_MISSING = """\
Sorry, but you have to configure the
    * CELERY_SECURITY_KEY
    * CELERY_SECURITY_CERTIFICATE, and the
    * CELERY_SECURITY_CERT_STORE
configuration settings to use the auth serializer.

Please see the configuration reference for more information.
"""


class IncompleteConfiguration(UserWarning):
    pass


def _disable_insecure_serializers(whitelist=[]):
    for name in set(registry._decoders.keys()) - set(whitelist):
        try:
            registry.disable(name)
        except SerializerNotInstalled:
            pass


def setup_security(allowed_serializers=[], key=None, cert=None, store=None,
        digest=None, serializer=None):
    """setup secure serialization"""
    conf = current_app.conf
    if conf.CELERY_TASK_SERIALIZER != "auth":
        return warn(IncompleteConfiguration(AUTH_DISABLED))

    try:
        from OpenSSL import crypto  # noqa
    except ImportError:
        raise ImproperlyConfigured(SSL_NOT_INSTALLED)

    key = key or conf.CELERY_SECURITY_KEY
    cert = cert or conf.CELERY_SECURITY_CERTIFICATE
    store = store or conf.CELERY_SECURITY_CERT_STORE
    digest = digest or conf.CELERY_SECURITY_DIGEST
    serializer = serializer or conf.CELERY_SECURITY_SERIALIZER

    if any(not v for v in (key, cert, store)):
        raise ImproperlyConfigured(SETTING_MISSING)

    _disable_insecure_serializers(allowed_serializers)
    with open(key) as kf:
        with open(cert) as cf:
            register_auth(kf.read(), cf.read(), store)
