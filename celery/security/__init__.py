# -*- coding: utf-8 -*-
"""Message Signing Serializer."""
from __future__ import absolute_import, unicode_literals

from kombu.serialization import (
    registry, disable_insecure_serializers as _disable_insecure_serializers,
)
from celery.exceptions import ImproperlyConfigured


CRYPTOGRAPHY_NOT_INSTALLED = """\
You need to install the cryptography library to use the auth serializer.
Please install by:

    $ pip install cryptography
"""

SECURITY_SETTING_MISSING = """\
Sorry, but you have to configure the
    * security_key
    * security_certificate, and the
    * security_cert_store
configuration settings to use the auth serializer.

Please see the configuration reference for more information.
"""

SETTING_MISSING = """\
You have to configure a special task serializer
for signing and verifying tasks:
    * task_serializer = 'auth'

You have to accept only tasks which are serialized with 'auth'.
There is no point in signing messages if they are not verified.
    * accept_content = ['auth']
"""

__all__ = ('setup_security',)

try:
    import cryptography  # noqa
except ImportError:
    raise ImproperlyConfigured(CRYPTOGRAPHY_NOT_INSTALLED)

from .serialization import register_auth  # noqa: need cryptography first


def setup_security(allowed_serializers=None, key=None, cert=None, store=None,
                   digest=None, serializer='json', app=None):
    """See :meth:`@Celery.setup_security`."""
    if app is None:
        from celery import current_app
        app = current_app._get_current_object()

    _disable_insecure_serializers(allowed_serializers)

    # check conf for sane security settings
    conf = app.conf
    if conf.task_serializer != 'auth' or conf.accept_content != ['auth']:
        raise ImproperlyConfigured(SETTING_MISSING)

    key = key or conf.security_key
    cert = cert or conf.security_certificate
    store = store or conf.security_cert_store
    digest = digest or conf.security_digest

    if not (key and cert and store):
        raise ImproperlyConfigured(SECURITY_SETTING_MISSING)

    with open(key, 'r') as kf:
        with open(cert, 'r') as cf:
            register_auth(kf.read(), cf.read(), store, digest, serializer)
    registry._set_default_serializer('auth')


def disable_untrusted_serializers(whitelist=None):
    _disable_insecure_serializers(allowed=whitelist)
