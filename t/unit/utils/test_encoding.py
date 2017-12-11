<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery.utils import encoding


class test_encoding:

    def test_safe_str(self):
        assert encoding.safe_str(object())
        assert encoding.safe_str('foo')

    def test_safe_repr(self):
        assert encoding.safe_repr(object())

        class foo:
            def __repr__(self):
                raise ValueError('foo')

        assert encoding.safe_repr(foo())
