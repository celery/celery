from __future__ import absolute_import

from nose import SkipTest

from celery.tests.utils import unittest


class SecurityCase(unittest.TestCase):

    def setUp(self):
        try:
            from OpenSSL import crypto  # noqa
        except ImportError:
            raise SkipTest("OpenSSL.crypto not installed")
