import pickle
from unittest import mock

from celery.beat import Service


class test_Service:

    def test___reduce__(self):

        s1 = Service(mock.sentinel.app,
                     max_interval=mock.sentinel.max_interval,
                     schedule_filename=mock.sentinel.schedule_filename,
                     scheduler_cls=mock.sentinel.scheduler_cls)

        s2 = pickle.loads(pickle.dumps(s1))

        assert s1.app == s2.app
        assert s1.max_interval == s2.max_interval
        assert s1.schedule_filename == s2.schedule_filename
        assert s1.scheduler_cls == s2.scheduler_cls
