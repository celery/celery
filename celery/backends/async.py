"""
    celery.backends.async
    ~~~~~~~~~~~~~~~~~~~~~

    Async backend support utilitites.

"""
from __future__ import absolute_import, unicode_literals

import socket

from collections import deque
from time import sleep
from weakref import WeakKeyDictionary

from kombu.syn import detect_environment
from kombu.utils import cached_property

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


class greenletDrainer(Drainer):
    spawn = None
    _g = None
    _stopped = False

    def run(self):
        while not self._stopped:
            try:
                self.result_consumer.drain_events(timeout=1)
            except socket.timeout:
                pass

    def start(self):
        if self._g is None:
            self._g = self.spawn(self.run)

    def stop(self):
        self._stopped = True

    def wait_for(self, p, wait, timeout=None):
        if self._g is None:
            self.start()
        if not p.ready:
            sleep(0)


@register_drainer('eventlet')
class eventletDrainer(greenletDrainer):

    @cached_property
    def spawn(self):
        from eventlet import spawn
        return spawn


@register_drainer('gevent')
class geventDrainer(greenletDrainer):

    @cached_property
    def spawn(self):
        from gevent import spawn
        return spawn


class AsyncBackendMixin(object):

    def _collect_into(self, result, bucket):
        self.result_consumer.buckets[result] = bucket

    def iter_native(self, result, no_ack=True, **kwargs):
        self._ensure_not_eager()

        results = result.results
        if not results:
            raise StopIteration()

        bucket = deque()
        for node in results:
            if node._cache:
                bucket.append(node)
            else:
                self._collect_into(node, bucket)

        for _ in self._wait_for_pending(result, no_ack=no_ack, **kwargs):
            while bucket:
                node = bucket.popleft()
                yield node.id, node._cache
        while bucket:
            node = bucket.popleft()
            yield node.id, node._cache

    def add_pending_result(self, result):
        if result.id not in self._pending_results:
            self._pending_results[result.id] = result
            self.result_consumer.consume_from(result.id)
        return result

    def remove_pending_result(self, result):
        self._pending_results.pop(result.id, None)
        self.on_result_fulfilled(result)
        return result

    def on_result_fulfilled(self, result):
        self.result_consumer.cancel_for(result.id)

    def wait_for_pending(self, result,
                         callback=None, propagate=True, **kwargs):
        self._ensure_not_eager()
        for _ in self._wait_for_pending(result, **kwargs):
            pass
        return result.maybe_throw(callback=callback, propagate=propagate)

    def _wait_for_pending(self, result,
                          timeout=None, on_interval=None, on_message=None,
                          **kwargs):
        return self.result_consumer._wait_for_pending(
            result, timeout=timeout,
            on_interval=on_interval, on_message=on_message,
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

    def start(self):
        raise NotImplementedError()

    def stop(self):
        pass

    def drain_events(self, timeout=None):
        raise NotImplementedError()

    def consume_from(self, task_id):
        raise NotImplementedError()

    def cancel_for(self, task_id):
        raise NotImplementedError()

    def _after_fork(self):
        self.buckets.clear()
        self.buckets = WeakKeyDictionary()
        self.on_message = None
        self.on_after_fork()

    def on_after_fork(self):
        pass

    def drain_events_until(self, p, timeout=None, on_interval=None):
        return self.drainer.drain_events_until(
            p, timeout=timeout, on_interval=on_interval)

    def _wait_for_pending(self, result,
                          timeout=None, on_interval=None, on_message=None,
                          **kwargs):
        self.on_wait_for_pending(result, timeout=timeout, **kwargs)
        prev_on_m, self.on_message = self.on_message, on_message
        try:
            for _ in self.drain_events_until(
                    result.on_ready, timeout=timeout,
                    on_interval=on_interval):
                yield
                sleep(0)
        except socket.timeout:
            raise TimeoutError('The operation timed out.')
        finally:
            self.on_message = prev_on_m

    def on_wait_for_pending(self, result, timeout=None, **kwargs):
        pass

    def on_out_of_band_result(self, message):
        self.on_state_change(message.payload, message)

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
                buckets.pop(result)
            except KeyError:
                pass
        sleep(0)
