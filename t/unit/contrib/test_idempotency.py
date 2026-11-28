import threading
import time

import pytest

from celery import signals, states
from celery.contrib.idempotency import claim_or_skip
from celery.contrib.testing.idempotency import assert_idempotent
from celery.exceptions import Ignore


class Store:
    """A shared store with an atomic claim, like Redis ``SET NX``."""

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.claims = set()
        self.sent = []
        self.done = set()

    def claim(self, key):
        with self.lock:
            if key in self.claims:
                return False
            self.claims.add(key)
            return True

    def observe(self):
        return sorted(self.sent)


def io():
    # Stands in for a database or network call, where a worker
    # process can be overtaken by another one.
    time.sleep(0.01)


class test_assert_idempotent:

    def setup_method(self):
        self.store = store = Store()

        @self.app.task(shared=False)
        def appends(order):
            store.sent.append(order)

        @self.app.task(shared=False)
        def checks_then_appends(order):
            if order not in store.done:
                io()
                store.sent.append(order)
                store.done.add(order)

        @self.app.task(bind=True, shared=False)
        def claims_then_appends(self, order):
            claim_or_skip(self, store.claim)
            io()
            store.sent.append(order)

        self.appends = appends
        self.checks_then_appends = checks_then_appends
        self.claims_then_appends = claims_then_appends

    def check(self, task, **kwargs):
        return assert_idempotent(
            task, (1,), observe=self.store.observe, reset=self.store.reset,
            **kwargs)

    def test_second_run_repeats_the_side_effect(self):
        with pytest.raises(AssertionError, match='a second run'):
            self.check(self.appends)

    def test_check_then_act_passes_one_after_the_other(self):
        self.check(self.checks_then_appends, concurrent=False)

    def test_check_then_act_fails_when_runs_overlap(self):
        with pytest.raises(AssertionError, match='at the same time'):
            self.check(self.checks_then_appends, rounds=1)

    def test_bodies_start_together_when_threads_start_apart(self, monkeypatch):
        class SlowToStart(threading.Thread):
            def start(self):
                super().start()
                time.sleep(0.05)

        monkeypatch.setattr(threading, 'Thread', SlowToStart)
        with pytest.raises(AssertionError, match='at the same time'):
            self.check(self.checks_then_appends, rounds=1)

    def test_claimed_task_passes(self):
        assert self.check(self.claims_then_appends) == [1]

    def test_concurrent_check_needs_reset(self):
        with pytest.raises(TypeError, match='reset'):
            assert_idempotent(self.appends, (1,), observe=self.store.observe)

    def test_runs_without_reset_when_not_concurrent(self):
        @self.app.task(shared=False)
        def noop():
            pass
        assert assert_idempotent(noop, observe=lambda: 'x',
                                 concurrent=False) == 'x'

    def test_failed_run_fails_the_check(self):
        @self.app.task(shared=False)
        def fails():
            raise KeyError('order')
        with pytest.raises(AssertionError, match='failed') as exc_info:
            assert_idempotent(fails, observe=list, concurrent=False)
        assert isinstance(exc_info.value.__cause__, KeyError)

    def test_failure_in_a_concurrent_run_is_raised(self):
        @self.app.task(shared=False)
        def fails_in_a_thread():
            if threading.current_thread() is not threading.main_thread():
                raise KeyError('order')
        with pytest.raises(AssertionError, match='failed'):
            assert_idempotent(fails_in_a_thread, observe=list, reset=list,
                              rounds=1)

    def test_claim_that_is_not_atomic_fails_when_runs_overlap(self):
        store = self.store

        def get_then_set(key):
            if key in store.claims:
                return False
            io()
            store.claims.add(key)
            return True

        @self.app.task(bind=True, shared=False)
        def claims_with_get_then_set(self, order):
            claim_or_skip(self, get_then_set)
            store.sent.append(order)

        with pytest.raises(AssertionError, match='at the same time'):
            self.check(claims_with_get_then_set, rounds=1)

    def test_rounds_reset_the_state(self):
        resets = []
        self.store.reset = lambda: (resets.append(1), Store.reset(self.store))
        self.check(self.claims_then_appends, rounds=3)
        assert len(resets) == 4

    def test_concurrent_runs_that_hang_fail_the_check(self):
        release = threading.Event()

        @self.app.task(shared=False)
        def hangs():
            if threading.current_thread() is not threading.main_thread():
                release.wait(5)
        try:
            with pytest.raises(AssertionError, match='did not finish'):
                assert_idempotent(hangs, observe=list, reset=list,
                                  rounds=1, timeout=0.2)
        finally:
            release.set()
            for thread in threading.enumerate():
                if thread.name.startswith('assert_idempotent-'):
                    thread.join(5)

    def test_disconnects_the_prerun_receiver(self):
        before = len(signals.task_prerun.receivers)
        self.check(self.claims_then_appends, rounds=1)
        assert len(signals.task_prerun.receivers) == before


class test_claim_or_skip:

    def setup_method(self):
        self.store = store = Store()

        @self.app.task(bind=True, shared=False)
        def charge(self, order):
            claim_or_skip(self, store.claim)
            store.sent.append(order)
            return 'charged'

        self.charge = charge

    def test_first_run_claims(self):
        result = self.charge.apply((1,), task_id='tid')
        assert result.state == states.SUCCESS
        assert self.store.claims == {f'celery-claim:{self.charge.name}:tid:0'}

    def test_duplicate_is_ignored(self):
        self.charge.apply((1,), task_id='tid')
        result = self.charge.apply((1,), task_id='tid')
        assert result.state == states.IGNORED
        assert isinstance(result.result, Ignore)
        assert self.store.sent == [1]

    def test_other_task_id_runs(self):
        self.charge.apply((1,), task_id='a')
        self.charge.apply((1,), task_id='b')
        assert self.store.sent == [1, 1]

    def test_retry_gets_a_new_claim(self):
        store = self.store

        @self.app.task(bind=True, shared=False, max_retries=1)
        def flaky(self):
            claim_or_skip(self, store.claim)
            store.sent.append(self.request.retries)
            if self.request.retries == 0:
                raise self.retry(countdown=0)

        assert flaky.apply().state == states.SUCCESS
        assert store.sent == [0, 1]

    def test_lost_run_is_not_repeated(self):
        # The claim is made before the side effect, so a run that stops
        # after claiming leaves the work undone: at most once.
        store = self.store

        @self.app.task(bind=True, shared=False)
        def stops(self):
            claim_or_skip(self, store.claim)
            if not store.done:
                store.done.add('stopped')
                raise RuntimeError('worker lost')
            store.sent.append(1)

        assert stops.apply(task_id='tid').state == states.FAILURE
        assert stops.apply(task_id='tid').state == states.IGNORED
        assert store.sent == []

    def test_custom_key(self):
        store = self.store

        @self.app.task(bind=True, shared=False)
        def by_order(self, order):
            claim_or_skip(self, store.claim, key=f'order:{order}')
            store.sent.append(order)

        by_order.apply((1,))
        assert by_order.apply((1,)).state == states.IGNORED
        assert store.claims == {'order:1'}

    def test_needs_a_task_id(self):
        with pytest.raises(RuntimeError, match='task id'):
            self.charge(1)
