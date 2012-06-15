from __future__ import absolute_import

from nose import SkipTest

from celery.tests.utils import Case


class SecurityCase(Case):

    def setUp(self):
        try:
            from OpenSSL import crypto  # noqa
        except ImportError:
            raise SkipTest('OpenSSL.crypto not installed')
