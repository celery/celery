from __future__ import absolute_import, unicode_literals

import os
from itertools import count

import pytest

from case import Mock, patch
from celery.concurrency.base import BasePool, apply_target
from celery.exceptions import WorkerShutdown, WorkerTerminate


class test_BasePool:

    def test_apply_target(self):

        scratch = {}
        counter = count(0)

        def gen_callback(name, retval=None):

            def callback(*args):
                scratch[name] = (next(counter), args)
                return retval

            return callback

        apply_target(gen_callback('target', 42),
                     args=(8, 16),
                     callback=gen_callback('callback'),
                     accept_callback=gen_callback('accept_callback'))

        assert scratch['target'] == (1, (8, 16))
        assert scratch['callback'] == (2, (42,))
        pa1 = scratch['accept_callback']
        assert pa1[0] == 0
        assert pa1[1][0] == os.getpid()
        assert pa1[1][1]

        # No accept callback
        scratch.clear()
        apply_target(gen_callback('target', 42),
                     args=(8, 16),
                     callback=gen_callback('callback'),
                     accept_callback=None)
        assert scratch == {
            'target': (3, (8, 16)),
            'callback': (4, (42,)),
        }

    def test_apply_target__propagate(self):
        target = Mock(name='target')
        target.side_effect = KeyError()
        with pytest.raises(KeyError):
            apply_target(target, propagate=(KeyError,))

    def test_apply_target__raises(self):
        target = Mock(name='target')
        target.side_effect = KeyError()
        with pytest.raises(KeyError):
            apply_target(target)

    def test_apply_target__raises_WorkerShutdown(self):
        target = Mock(name='target')
        target.side_effect = WorkerShutdown()
        with pytest.raises(WorkerShutdown):
            apply_target(target)

    def test_apply_target__raises_WorkerTerminate(self):
        target = Mock(name='target')
        target.side_effect = WorkerTerminate()
        with pytest.raises(WorkerTerminate):
            apply_target(target)

    def test_apply_target__raises_BaseException(self):
        target = Mock(name='target')
        callback = Mock(name='callback')
        target.side_effect = BaseException()
        apply_target(target, callback=callback)
        callback.assert_called()

    @patch('celery.concurrency.base.reraise')
    def test_apply_target__raises_BaseException_raises_else(self, reraise):
        target = Mock(name='target')
        callback = Mock(name='callback')
        reraise.side_effect = KeyError()
        target.side_effect = BaseException()
        with pytest.raises(KeyError):
            apply_target(target, callback=callback)
        callback.assert_not_called()

    def test_does_not_debug(self):
        x = BasePool(10)
        x._does_debug = False
        x.apply_async(object)

    def test_num_processes(self):
        assert BasePool(7).num_processes == 7

    def test_interface_on_start(self):
        BasePool(10).on_start()

    def test_interface_on_stop(self):
        BasePool(10).on_stop()

    def test_interface_on_apply(self):
        BasePool(10).on_apply()

    def test_interface_info(self):
        assert BasePool(10).info == {
            'max-concurrency': 10,
        }

    def test_interface_flush(self):
        assert BasePool(10).flush() is None

    def test_active(self):
        p = BasePool(10)
        assert not p.active
        p._state = p.RUN
        assert p.active

    def test_restart(self):
        p = BasePool(10)
        with pytest.raises(NotImplementedError):
            p.restart()

    def test_interface_on_terminate(self):
        p = BasePool(10)
        p.on_terminate()

    def test_interface_terminate_job(self):
        with pytest.raises(NotImplementedError):
            BasePool(10).terminate_job(101)

    def test_interface_did_start_ok(self):
        assert BasePool(10).did_start_ok()

    def test_interface_register_with_event_loop(self):
        assert BasePool(10).register_with_event_loop(Mock()) is None

    def test_interface_on_soft_timeout(self):
        assert BasePool(10).on_soft_timeout(Mock()) is None

    def test_interface_on_hard_timeout(self):
        assert BasePool(10).on_hard_timeout(Mock()) is None

    def test_interface_close(self):
        p = BasePool(10)
        p.on_close = Mock()
        p.close()
        assert p._state == p.CLOSE
        p.on_close.assert_called_with()

    def test_interface_no_close(self):
        assert BasePool(10).on_close() is None
