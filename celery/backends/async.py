"""Async I/O backend support utilities."""
import socket
import threading
from collections import deque
from time import monotonic, sleep
from typing import (
    Any, Awaitable, Callable, Iterable, Iterator, Mapping, Set, Sequence,
)
from queue import Empty
from weakref import WeakKeyDictionary

from kombu.types import MessageT
from kombu.utils.compat import detect_environment
from kombu.utils.objects import cached_property

from celery import states
from celery.exceptions import TimeoutError
from celery.types import AppT, BackendT, ResultT, ResultConsumerT
from celery.utils.collections import BufferMap

from .base import pending_results_t

__all__ = (
    'AsyncBackendMixin', 'BaseResultConsumer', 'Drainer',
    'register_drainer',
)

drainers = {}


def register_drainer(name: str) -> Callable:
    """Decorator used to register a new result drainer type."""
    def _inner(cls: type) -> type:
        drainers[name] = cls
        return cls
    return _inner


@register_drainer('default')
class Drainer:
    """Result draining service."""

    def __init__(self, result_consumer: ResultConsumerT) -> None:
        self.result_consumer = result_consumer

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def drain_events_until(self, p: Awaitable,
                           timeout: float = None,
                           on_interval: Callable = None,
                           wait: Callable = None) -> Iterator:
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

    def wait_for(self, p: Awaitable, wait: Callable,
                 timeout: float = None) -> None:
        wait(timeout=timeout)


class greenletDrainer(Drainer):
    spawn = None
    _g = None

    def __init__(self, *args, **kwargs) -> None:
        super(greenletDrainer, self).__init__(*args, **kwargs)
        self._started = threading.Event()
        self._stopped = threading.Event()
        self._shutdown = threading.Event()

    def run(self) -> None:
        self._started.set()
        while not self._stopped.is_set():
            try:
                self.result_consumer.drain_events(timeout=1)
            except socket.timeout:
                pass
        self._shutdown.set()

    def start(self) -> None:
        if not self._started.is_set():
            self._g = self.spawn(self.run)
            self._started.wait()

    def stop(self) -> None:
        self._stopped.set()
        self._shutdown.wait(threading.TIMEOUT_MAX)

    def wait_for(self, p: Awaitable, wait: Callable,
                 timeout: float = None) -> None:
        self.start()
        if not p.ready:
            sleep(0)


@register_drainer('eventlet')
class eventletDrainer(greenletDrainer):

    @cached_property
    def spawn(self) -> Callable:
        from eventlet import spawn
        return spawn


@register_drainer('gevent')
class geventDrainer(greenletDrainer):

    @cached_property
    def spawn(self) -> Callable:
        from gevent import spawn
        return spawn


class AsyncBackendMixin:
    """Mixin for backends that enables the async API."""

    def _collect_into(self, result: ResultT, bucket: deque):
        self.result_consumer.buckets[result] = bucket

    async def iter_native(
            self, result: ResultT,
            *,
            no_ack: bool = True, **kwargs) -> Iterator[str, Mapping]:
        self._ensure_not_eager()

        results = result.results
        if not results:
            raise StopIteration()

        # we tell the result consumer to put consumed results
        # into these buckets.
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

    def add_pending_result(self, result: ResultT,
                           weak: bool = False,
                           start_drainer: bool = True) -> ResultT:
        if start_drainer:
            self.result_consumer.drainer.start()
        try:
            self._maybe_resolve_from_buffer(result)
        except Empty:
            self._add_pending_result(result.id, result, weak=weak)
        return result

    def _maybe_resolve_from_buffer(self, result: ResultT) -> None:
        result._maybe_set_cache(self._pending_messages.take(result.id))

    def _add_pending_result(self, task_id: str, result: ResultT,
                            weak: bool = False) -> None:
        concrete, weak_ = self._pending_results
        if task_id not in weak_ and result.id not in concrete:
            (weak_ if weak else concrete)[task_id] = result
            self.result_consumer.consume_from(task_id)

    def add_pending_results(self, results: Sequence[ResultT],
                            weak: bool = False) -> None:
        self.result_consumer.drainer.start()
        [self.add_pending_result(result, weak=weak, start_drainer=False)
         for result in results]

    def remove_pending_result(self, result: ResultT) -> ResultT:
        self._remove_pending_result(result.id)
        self.on_result_fulfilled(result)
        return result

    def _remove_pending_result(self, task_id: str) -> None:
        for map in self._pending_results:
            map.pop(task_id, None)

    def on_result_fulfilled(self, result: ResultT) -> None:
        self.result_consumer.cancel_for(result.id)

    def wait_for_pending(self, result: ResultT,
                         callback: Callable = None,
                         propagate: bool = True,
                         **kwargs) -> Any:
        self._ensure_not_eager()
        for _ in self._wait_for_pending(result, **kwargs):
            pass
        return result.maybe_throw(callback=callback, propagate=propagate)

    def _wait_for_pending(self, result: ResultT,
                          timeout: float = None,
                          on_interval: Callable = None,
                          on_message: Callable = None,
                          **kwargs) -> Iterable:
        return self.result_consumer._wait_for_pending(
            result, timeout=timeout,
            on_interval=on_interval, on_message=on_message,
        )

    @property
    def is_async(self) -> bool:
        return True


class BaseResultConsumer:
    """Manager responsible for consuming result messages."""

    def __init__(self, backend: BackendT, app: AppT, accept: Set[str],
                 pending_results: pending_results_t,
                 pending_messages: BufferMap) -> None:
        self.backend = backend
        self.app = app
        self.accept = accept
        self._pending_results = pending_results
        self._pending_messages = pending_messages
        self.on_message = None
        self.buckets = WeakKeyDictionary()
        self.drainer = drainers[detect_environment()](self)

    def start(self, initial_task_id: str, **kwargs) -> None:
        raise NotImplementedError()

    def stop(self) -> None:
        ...

    def drain_events(self, timeout: float = None) -> None:
        raise NotImplementedError()

    def consume_from(self, task_id: str) -> None:
        raise NotImplementedError()

    def cancel_for(self, task_id: str) -> None:
        raise NotImplementedError()

    def _after_fork(self) -> None:
        self.buckets.clear()
        self.buckets = WeakKeyDictionary()
        self.on_message = None
        self.on_after_fork()

    def on_after_fork(self) -> None:
        ...

    def drain_events_until(self, p: Awaitable,
                           timeout: float = None,
                           on_interval: Callable = None) -> Iterable:
        return self.drainer.drain_events_until(
            p, timeout=timeout, on_interval=on_interval)

    def _wait_for_pending(self, result,
                          timeout=None, on_interval=None, on_message=None,
                          **kwargs) -> Iterable:
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

    def on_wait_for_pending(self, result: ResultT,
                            timeout: float = None, **kwargs) -> None:
        ...

    def on_out_of_band_result(self, message: MessageT) -> None:
        self.on_state_change(message.payload, message)

    def _get_pending_result(self, task_id: str) -> ResultT:
        for mapping in self._pending_results:
            try:
                return mapping[task_id]
            except KeyError:
                pass
        raise KeyError(task_id)

    def on_state_change(self, meta: Mapping, message: MessageT) -> None:
        if self.on_message:
            self.on_message(meta)
        if meta['status'] in states.READY_STATES:
            task_id = meta['task_id']
            try:
                result = self._get_pending_result(task_id)
            except KeyError:
                # send to buffer in case we received this result
                # before it was added to _pending_results.
                self._pending_messages.put(task_id, meta)
            else:
                result._maybe_set_cache(meta)
                buckets = self.buckets
                try:
                    # remove bucket for this result, since it's fulfilled
                    bucket = buckets.pop(result)
                except KeyError:
                    pass
                else:
                    # send to waiter via bucket
                    bucket.append(result)
        sleep(0)
