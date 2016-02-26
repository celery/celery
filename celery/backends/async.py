"""
    celery.backends.async
    ~~~~~~~~~~~~~~~~~~~~~

    Async backend support utilitites.

"""
from __future__ import absolute_import, unicode_literals

import socket
import time

from collections import deque
from weakref import WeakKeyDictionary

from kombu.syn import detect_environment

from celery import states
from celery.exceptions import TimeoutError
from celery.five import monotonic

drainers = {}


def register_drainer(name):

    def _inner(cls):
        drainers[name] = cls
        return cls
    return _inner


@register_drainer('default')
class Drainer(object):

    def __init__(self, result_consumer):
        self.result_consumer = result_consumer

    def drain_events_until(self, p, timeout=None, on_interval=None,
                           monotonic=monotonic, wait=None):
        wait = wait or self.result_consumer.drain_events
        time_start = monotonic()

        while 1:
            # Total time spent may exceed a single call to wait()
            if timeout and monotonic() - time_start >= timeout:
                raise socket.timeout()
            try:
                yield self.wait_for(p, wait, timeout=1)
            except socket.timeout:
                pass
            if on_interval:
                on_interval()
            if p.ready:  # got event on the wanted channel.
                break

    def wait_for(self, p, wait, timeout=None):
        wait(timeout=timeout)


@register_drainer('eventlet')
class EventletDrainer(Drainer):
    _g = None
    _stopped = False

    def run(self):
        while not self._stopped:
            try:
                self.result_consumer.drain_events(timeout=10)
            except socket.timeout:
                pass

    def start(self):
        from eventlet import spawn
        if self._g is None:
            self._g = spawn(self.run)

    def stop(self):
        self._stopped = True

    def wait_for(self, p, wait, timeout=None):
        if self._g is None:
            self.start()
        if not p.ready:
            time.sleep(0)


class AsyncBackendMixin(object):

    def _collect_into(self, result, bucket):
        self.result_consumer.buckets[result] = bucket

    def iter_native(self, result, timeout=None, interval=0.5, no_ack=True,
                    on_message=None, on_interval=None):
        results = result.results
        if not results:
            raise StopIteration()

        bucket = deque()
        for result in results:
            if result._cache:
                bucket.append(result)
            else:
                self._collect_into(result, bucket)

        for _ in self._wait_for_pending(
                result,
                timeout=timeout, interval=interval, no_ack=no_ack,
                on_message=on_message, on_interval=on_interval):
            while bucket:
                result = bucket.popleft()
                yield result.id, result._cache
        while bucket:
            result = bucket.popleft()
            yield result.id, result._cache

    def add_pending_result(self, result):
        if result.id not in self._pending_results:
            self._pending_results[result.id] = result
            self.result_consumer.consume_from(self._create_binding(result.id))
        return result

    def remove_pending_result(self, result):
        self._pending_results.pop(result.id, None)
        self.on_result_fulfilled(result)
        return result

    def on_result_fulfilled(self, result):
        pass

    def wait_for_pending(self, result,
                         callback=None, propagate=True, **kwargs):
        for _ in self._wait_for_pending(result, **kwargs):
            pass
        return result.maybe_throw(callback=callback, propagate=propagate)

    def _wait_for_pending(self, result, timeout=None, interval=0.5,
                          no_ack=True, on_interval=None, on_message=None,
                          callback=None, propagate=True):
        return self.result_consumer._wait_for_pending(
            result, timeout=timeout, interval=interval,
            no_ack=no_ack, on_interval=on_interval,
            callback=callback, on_message=on_message, propagate=propagate,
        )

    @property
    def is_async(self):
        return True


class BaseResultConsumer(object):

    def __init__(self, backend, app, accept, pending_results):
        self.backend = backend
        self.app = app
        self.accept = accept
        self._pending_results = pending_results
        self.on_message = None
        self.buckets = WeakKeyDictionary()
        self.drainer = drainers[detect_environment()](self)

    def drain_events(self, timeout=None):
        raise NotImplementedError('subclass responsibility')

    def _after_fork(self):
        self.bucket.clear()
        self.buckets = WeakKeyDictionary()
        self.on_message = None
        self.on_after_fork()

    def on_after_fork(self):
        pass

    def drain_events_until(self, p, timeout=None, on_interval=None):
        return self.drainer.drain_events_until(
            p, timeout=timeout, on_interval=on_interval)

    def _wait_for_pending(self, result, timeout=None, interval=0.5,
                          no_ack=True, on_interval=None, callback=None,
                          on_message=None, propagate=True):
        prev_on_m, self.on_message = self.on_message, on_message
        try:
            for _ in self.drain_events_until(
                    result.on_ready, timeout=timeout,
                    on_interval=on_interval):
                yield
                time.sleep(0)
        except socket.timeout:
            raise TimeoutError('The operation timed out.')
        finally:
            self.on_message = prev_on_m

    def on_state_change(self, meta, message):
        if self.on_message:
            self.on_message(meta)
        if meta['status'] in states.READY_STATES:
            try:
                result = self._pending_results[meta['task_id']]
            except KeyError:
                return
            result._maybe_set_cache(meta)
            buckets = self.buckets
            try:
                buckets[result].append(result)
                buckets.pop(result)
            except KeyError:
                pass
        time.sleep(0)
