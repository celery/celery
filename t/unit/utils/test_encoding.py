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
