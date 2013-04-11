# -*- coding: utf-8 -*-
"""
    celery.security
    ~~~~~~~~~~~~~~~

    Module implementing the signing message serializer.

"""
from __future__ import absolute_import
from __future__ import with_statement

from kombu.serialization import registry

from celery import current_app
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
                   digest='sha1', serializer='json'):
    """Setup the message-signing serializer.

    Disables untrusted serializers and if configured to use the ``auth``
    serializer will register the auth serializer with the provided settings
    into the Kombu serializer registry.

    :keyword allowed_serializers:  List of serializer names, or content_types
        that should be exempt from being disabled.
    :keyword key: Name of private key file to use.
        Defaults to the :setting:`CELERY_SECURITY_KEY` setting.
    :keyword cert: Name of certificate file to use.
        Defaults to the :setting:`CELERY_SECURITY_CERTIFICATE` setting.
    :keyword store: Directory containing certificates.
        Defaults to the :setting:`CELERY_SECURITY_CERT_STORE` setting.
    :keyword digest: Digest algorithm used when signing messages.
        Default is ``sha1``.
    :keyword serializer: Serializer used to encode messages after
        they have been signed.  See :setting:`CELERY_TASK_SERIALIZER` for
        the serializers supported.
        Default is ``json``.

    """

    disable_untrusted_serializers(allowed_serializers)

    conf = current_app.conf
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
            register_auth(kf.read(), cf.read(), store)
