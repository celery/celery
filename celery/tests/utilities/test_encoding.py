from __future__ import absolute_import

from celery.utils import encoding
from celery.tests.utils import Case


class test_encoding(Case):

    def test_safe_str(self):
        self.assertTrue(encoding.safe_str(object()))
        self.assertTrue(encoding.safe_str('foo'))
        self.assertTrue(encoding.safe_str(u'foo'))

    def test_safe_repr(self):
        self.assertTrue(encoding.safe_repr(object()))

        class foo(object):
            def __repr__(self):
                raise ValueError('foo')

        self.assertTrue(encoding.safe_repr(foo()))
