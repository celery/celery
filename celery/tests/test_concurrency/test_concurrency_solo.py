from __future__ import absolute_import

import operator

from celery.concurrency import solo
from celery.utils import noop
from celery.tests.utils import Case


class test_solo_TaskPool(Case):

    def test_on_start(self):
        x = solo.TaskPool()
        x.on_start()

    def test_on_apply(self):
        x = solo.TaskPool()
        x.on_start()
        x.on_apply(operator.add, (2, 2), {}, noop, noop)

    def test_info(self):
        x = solo.TaskPool()
        x.on_start()
        self.assertTrue(x.info)
