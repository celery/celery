from __future__ import absolute_import
from __future__ import with_statement

from kombu.serialization import unregister, SerializerNotInstalled

from .. import current_app
from ..exceptions import ImproperlyConfigured

from .serialization import register_auth


def _disable_insecure_serializers():
    for name in ("pickle", "json", "yaml", "msgpack"):
        try:
            unregister(name)
        except SerializerNotInstalled:
            pass


def setup_security():
    """setup secure serialization"""
    conf = current_app.conf
    if conf.CELERY_TASK_SERIALIZER != "auth":
        return

    try:
        from OpenSSL import crypto  # noqa
    except ImportError:
        raise ImproperlyConfigured(
            "You need to install the pyOpenSSL library to use "
            "the auth serializer.")

    key = conf.CELERY_SECURITY_KEY
    cert = conf.CELERY_SECURITY_CERTIFICATE
    store = conf.CELERY_SECURITY_CERT_STORE

    if key is None or cert is None or store is None:
        raise ImproperlyConfigured(
            "CELERY_SECURITY_KEY, CELERY_SECURITY_CERTIFICATE and "
            "CELERY_SECURITY_CERT_STORE options are required "
            "settings when using the auth serializer")

    with open(key) as kf:
        with open(cert) as cf:
            register_auth(kf.read(), cf.read(), store)
    _disable_insecure_serializers()
