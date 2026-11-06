import operator
from unittest.mock import Mock, patch

import pytest

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

    def test_on_worker_process_init_sender_is_hostname(self):
        on_worker_process_init = Mock()
        signals.worker_process_init.connect(on_worker_process_init)
        try:
            solo.TaskPool(initargs=(Mock(), 'solo@example.com'))
            assert on_worker_process_init.call_args.kwargs['sender'] == 'solo@example.com'
        finally:
            signals.worker_process_init.disconnect(on_worker_process_init)

    def test_on_worker_process_init_sender_without_initargs(self):
        on_worker_process_init = Mock()
        signals.worker_process_init.connect(on_worker_process_init)
        try:
            solo.TaskPool()
            assert on_worker_process_init.call_args.kwargs['sender'] is None
        finally:
            signals.worker_process_init.disconnect(on_worker_process_init)

    def test_on_apply_logs_an_error_from_the_callback(self):
        # The callback runs in the consumer.  An error from it, such as the
        # result backend being down, must not reach the consumer (#11018).
        x = solo.TaskPool()
        callback = Mock(side_effect=ConnectionError('backend down'))
        with patch('celery.concurrency.solo.logger') as logger:
            x.apply_async(operator.add, (2, 2), callback=callback)
        callback.assert_called_once_with(4)
        logger.error.assert_called_once()
        assert isinstance(logger.error.call_args[0][1], ConnectionError)

    def test_on_apply_raises_MemoryError_from_the_callback(self):
        x = solo.TaskPool()
        callback = Mock(side_effect=MemoryError())
        with pytest.raises(MemoryError):
            x.apply_async(operator.add, (2, 2), callback=callback)

    def test_on_apply_raises_callbacks_propagate_from_the_callback(self):
        x = solo.TaskPool(callbacks_propagate=(KeyError,))
        callback = Mock(side_effect=KeyError('propagated'))
        with pytest.raises(KeyError):
            x.apply_async(operator.add, (2, 2), callback=callback)
