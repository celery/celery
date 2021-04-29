import pickle
from datetime import datetime

from celery.exceptions import Reject, Retry
from celery.utils.serialization import UnpickleableExceptionWrapper


class test_Retry:

    def test_when_datetime(self):
        x = Retry('foo', KeyError(), when=datetime.utcnow())
        assert x.humanize()

    def test_pickleable(self):
        x = Retry('foo', KeyError(), when=datetime.utcnow())
        y = pickle.loads(pickle.dumps(x))
        assert repr(x) == repr(y)

    def test_wrap_unpickleable_exception(self):
        class Danger(Exception):
            def __reduce__(self):
                raise RuntimeError("reduce is broken")
        x = Retry('foo', Danger(), when=datetime.utcnow())
        y = pickle.loads(pickle.dumps(x))
        assert isinstance(y.exc, UnpickleableExceptionWrapper)




class test_Reject:

    def test_attrs(self):
        x = Reject('foo', requeue=True)
        assert x.reason == 'foo'
        assert x.requeue

    def test_repr(self):
        assert repr(Reject('foo', True))

    def test_pickleable(self):
        x = Retry('foo', True)
        assert pickle.loads(pickle.dumps(x))
