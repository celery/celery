from celery.tests.case import AppCase, skip


@skip.unless_module('OpenSSL.crypto', name='pyOpenSSL')
class SecurityCase(AppCase):
    pass
