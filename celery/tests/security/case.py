from __future__ import absolute_import

from nose import SkipTest

from celery.tests.case import AppCase


class SecurityCase(AppCase):

    def setup(self):
        try:
            from OpenSSL import crypto  # noqa
        except ImportError:
            raise SkipTest('OpenSSL.crypto not installed')
