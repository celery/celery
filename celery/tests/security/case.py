from __future__ import absolute_import

from celery.tests.case import AppCase, SkipTest


class SecurityCase(AppCase):

    def setup(self):
        try:
            from OpenSSL import crypto  # noqa
        except ImportError:
            raise SkipTest('OpenSSL.crypto not installed')
