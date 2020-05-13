from __future__ import absolute_import, unicode_literals

import operator

from case import Mock

from celery import signals
from celery.concurrency import solo
from celery.utils.functional import noop


class test_solo_TaskPool:

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
        assert x.info

    def test_on_worker_process_init_called(self):
        """Upon the initialization of a new solo worker pool a worker_process_init
        signal should be emitted"""
        on_worker_process_init = Mock()
        signals.worker_process_init.connect(on_worker_process_init)
        solo.TaskPool()
        assert on_worker_process_init.call_count == 1
