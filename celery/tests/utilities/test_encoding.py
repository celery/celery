from __future__ import absolute_import, unicode_literals

import sys

from nose import SkipTest

from celery.five import string
from celery.utils import encoding
from celery.tests.utils import Case


class test_encoding(Case):

    def test_safe_str(self):
        self.assertTrue(encoding.safe_str(object()))
        self.assertTrue(encoding.safe_str('foo'))

    def test_safe_str_UnicodeDecodeError(self):
        if sys.version_info >= (3, 0):
            raise SkipTest('py3k: not relevant')

        class foo(string):

            def encode(self, *args, **kwargs):
                raise UnicodeDecodeError('foo')

        self.assertIn('<Unrepresentable', encoding.safe_str(foo()))

    def test_safe_repr(self):
        self.assertTrue(encoding.safe_repr(object()))

        class foo(object):
            def __repr__(self):
                raise ValueError('foo')

        self.assertTrue(encoding.safe_repr(foo()))
