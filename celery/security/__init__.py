from __future__ import absolute_import
from __future__ import with_statement

from kombu.serialization import registry, SerializerNotInstalled

from .. import current_app
from ..exceptions import ImproperlyConfigured

from .serialization import register_auth

SSL_NOT_INSTALLED = """\
You need to install the pyOpenSSL library to use the auth serializer.
Please install by:

    $ pip install pyOpenSSL
"""

SETTING_MISSING = """\
Sorry, but you have to configure the
    * CELERY_SECURITY_KEY
    * CELERY_SECURITY_CERTIFICATE, and the
    * CELERY_SECURITY_CERT_STORE
configuration settings to use the auth serializer.

Please see the configuration reference for more information.
"""


def _disable_insecure_serializers(whitelist=None):
    for name in set(registry._decoders.keys()) - set(whitelist or []):
        try:
            registry.disable(name)
        except SerializerNotInstalled:
            pass


def setup_security(allowed_serializers=None, key=None, cert=None, store=None,
        digest="sha1", serializer="json"):
    """setup secure serialization"""
    _disable_insecure_serializers(allowed_serializers)

    conf = current_app.conf
    if conf.CELERY_TASK_SERIALIZER != "auth":
        return

    try:
        from OpenSSL import crypto  # noqa
    except ImportError:
        raise ImproperlyConfigured(SSL_NOT_INSTALLED)

    key = key or conf.CELERY_SECURITY_KEY
    cert = cert or conf.CELERY_SECURITY_CERTIFICATE
    store = store or conf.CELERY_SECURITY_CERT_STORE

    if any(not v for v in (key, cert, store)):
        raise ImproperlyConfigured(SETTING_MISSING)

    with open(key) as kf:
        with open(cert) as cf:
            register_auth(kf.read(), cf.read(), store)
