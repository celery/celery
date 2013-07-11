# -*- coding: utf-8 -*-
"""
    celery.security
    ~~~~~~~~~~~~~~~

    Module implementing the signing message serializer.

"""
from __future__ import absolute_import

from kombu.serialization import registry

from celery.exceptions import ImproperlyConfigured

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


def disable_untrusted_serializers(whitelist=None):
    for name in set(registry._decoders) - set(whitelist or []):
        registry.disable(name)


def setup_security(allowed_serializers=None, key=None, cert=None, store=None,
                   digest='sha1', serializer='json', app=None):
    """See :meth:`@Celery.setup_security`."""
    if app is None:
        from celery import current_app
        app = current_app._get_current_object()

    disable_untrusted_serializers(allowed_serializers)

    conf = app.conf
    if conf.CELERY_TASK_SERIALIZER != 'auth':
        return

    try:
        from OpenSSL import crypto  # noqa
    except ImportError:
        raise ImproperlyConfigured(SSL_NOT_INSTALLED)

    key = key or conf.CELERY_SECURITY_KEY
    cert = cert or conf.CELERY_SECURITY_CERTIFICATE
    store = store or conf.CELERY_SECURITY_CERT_STORE

    if not (key and cert and store):
        raise ImproperlyConfigured(SETTING_MISSING)

    with open(key) as kf:
        with open(cert) as cf:
            register_auth(kf.read(), cf.read(), store, digest, serializer)
    registry._set_default_serializer('auth')
