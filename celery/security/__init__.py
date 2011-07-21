from kombu.serialization import unregister, SerializerNotInstalled

from celery.app import app_or_default
from celery.security.serialization import register_auth
from celery.exceptions import ImproperlyConfigured

def _disable_insecure_serializers():
    for name in ('pickle', 'json', 'yaml', 'msgpack'):
        try:
            unregister(name)
        except SerializerNotInstalled:
            pass

def setup_security():
    """setup secure serialization"""
    conf = app_or_default().conf
    if conf.CELERY_TASK_SERIALIZER != 'auth':
        return

    key = getattr(conf, 'CELERY_SECURITY_KEY', None)
    cert = getattr(conf, 'CELERY_SECURITY_CERTIFICATE', None)
    store = getattr(conf, 'CELERY_SECURITY_CERT_STORE', None)

    if key is None or cert is None or store is None:
        raise ImproperlyConfigured(
            "CELERY_SECURITY_KEY, CELERY_SECURITY_CERTIFICATE and "
            "CELERY_SECURITY_CERT_STORE options are required "
            "for auth serializer")

    with open(key) as kf, open(cert) as cf:
        register_auth(kf.read(), cf.read(), store)
    _disable_insecure_serializers()
