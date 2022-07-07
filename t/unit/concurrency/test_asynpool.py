"""Tests for celery.concurrency.asynpool."""

from os import getpid

from kombu.asynchronous import Hub

from celery.concurrency.asynpool import AsynPool


def check_if_state_persisted():
    from t.unit.concurrency import test_asynpool

    return getpid(), hasattr(test_asynpool, "only_in_parent_process")


class test_AsynPool:
    def test_subprocesses_are_spawned(self):
        """
        Subprocesses are new, indepenent processes, rather than
        buggy fork()-without-execve() subprocesses.
        """
        # Add some state to this module, in the parent; only if we're
        # doing fork() without execve() will this be visible to worker
        # processes.
        global only_in_parent_process
        only_in_parent_process = True

        pool = AsynPool(processes=1, threads=False)
        hub = Hub()
        pool.register_with_event_loop(hub)
        result = pool.apply_async(check_if_state_persisted)
        for i in range(100):
            if result.ready():
                break
            hub.run_once()
        assert result.ready()
        del only_in_parent_process

        worker_pid, state_leaked_to_worker = result.get()
        assert getpid() != worker_pid
        assert not state_leaked_to_worker
