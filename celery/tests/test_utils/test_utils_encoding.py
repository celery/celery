from __future__ import absolute_import

import sys

from nose import SkipTest

from celery.utils import encoding
from celery.tests.utils import unittest


class test_encoding(unittest.TestCase):

    def test_safe_str(self):
        self.assertTrue(encoding.safe_str(object()))
        self.assertTrue(encoding.safe_str("foo"))
        self.assertTrue(encoding.safe_str(u"foo"))

    def test_safe_str_UnicodeDecodeError(self):
        if sys.version_info >= (3, 0):
            raise SkipTest("py3k: not relevant")

        class foo(unicode):

            def encode(self, *args, **kwargs):
                raise UnicodeDecodeError("foo")

        self.assertIn("<Unrepresentable", encoding.safe_str(foo()))

    def test_safe_repr(self):
        self.assertTrue(encoding.safe_repr(object()))

        class foo(object):
            def __repr__(self):
                raise ValueError("foo")

        self.assertTrue(encoding.safe_repr(foo()))
