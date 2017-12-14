# -*- coding: utf-8 -*-
"""Message Signing Serializer."""
from __future__ import absolute_import, unicode_literals
from kombu.serialization import (
    registry, disable_insecure_serializers as _disable_insecure_serializers,
)
from celery.exceptions import ImproperlyConfigured
from .serialization import register_auth

SSL_NOT_INSTALLED = """\
You need to install the pyOpenSSL library to use the auth serializer.
Please install by:

    $ pip install pyOpenSSL
"""

SETTING_MISSING = """\
Sorry, but you have to configure the
    * security_key
    * security_certificate, and the
    * security_cert_storE
configuration settings to use the auth serializer.

Please see the configuration reference for more information.
"""

__all__ = ('setup_security',)


def setup_security(allowed_serializers=None, key=None, cert=None, store=None,
                   digest='sha1', serializer='json', app=None):
    """See :meth:`@Celery.setup_security`."""
    if app is None:
        from celery import current_app
        app = current_app._get_current_object()

    _disable_insecure_serializers(allowed_serializers)

    conf = app.conf
    if conf.task_serializer != 'auth':
        return

    try:
        from OpenSSL import crypto  # noqa
    except ImportError:
        raise ImproperlyConfigured(SSL_NOT_INSTALLED)

    key = key or conf.security_key
    cert = cert or conf.security_certificate
    store = store or conf.security_cert_store

    if not (key and cert and store):
        raise ImproperlyConfigured(SETTING_MISSING)

    with open(key) as kf:
        with open(cert) as cf:
            register_auth(kf.read(), cf.read(), store, digest, serializer)
    registry._set_default_serializer('auth')


def disable_untrusted_serializers(whitelist=None):
    _disable_insecure_serializers(allowed=whitelist)
