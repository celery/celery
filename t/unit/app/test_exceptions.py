from __future__ import absolute_import, unicode_literals

import pickle

from datetime import datetime

from celery.exceptions import Reject, Retry


class test_Retry:

    def test_when_datetime(self):
        x = Retry('foo', KeyError(), when=datetime.utcnow())
        assert x.humanize()

    def test_pickleable(self):
        x = Retry('foo', KeyError(), when=datetime.utcnow())
        assert pickle.loads(pickle.dumps(x))


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
