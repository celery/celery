from __future__ import absolute_import

import pickle

from datetime import datetime

from celery.exceptions import RetryTaskError

from celery.tests.case import Case


class test_RetryTaskError(Case):

    def test_when_datetime(self):
        x = RetryTaskError('foo', KeyError(), when=datetime.utcnow())
        self.assertTrue(x.humanize())

    def test_pickleable(self):
        x = RetryTaskError('foo', KeyError(), when=datetime.utcnow())
        self.assertTrue(pickle.loads(pickle.dumps(x)))
