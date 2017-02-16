# -*- coding: utf-8 -*-
"""WorkController can be used to instantiate in-process workers.

The command-line interface for the worker is in :mod:`celery.bin.worker`,
while the worker program is in :mod:`celery.apps.worker`.

The worker program is responsible for adding signal handlers,
setting up logging, etc.  This is a bare-bones worker without
global side-effects (i.e., except for the global state stored in
:mod:`celery.worker.state`).

The worker consists of several components, all managed by bootsteps
(mod:`celery.bootsteps`).
"""
import os
import sys

from typing import Any, Callable, Mapping, Set, Sequence, Union

from billiard import cpu_count
from kombu.utils.compat import detect_environment

from celery import bootsteps
from celery.bootsteps import RUN, TERMINATE
from celery import concurrency as _concurrency
from celery import signals
from celery.exceptions import (
    ImproperlyConfigured, WorkerTerminate, TaskRevokedError,
)
from celery.types import (
    AppT, BlueprintT, LoopT, PoolT, RequestT, StepT, WorkerConsumerT,
)
from celery.platforms import EX_FAILURE, Pidfile, create_pidlock
from celery.utils.imports import reload_from_cwd
from celery.utils.log import mlevel, worker_logger as logger
from celery.utils.nodenames import default_nodename, worker_direct
from celery.utils.text import str_to_list
from celery.utils.threads import default_socket_timeout

from . import state

try:
    import resource
except ImportError:  # pragma: no cover
    resource = None  # noqa

__all__ = ['WorkController']

#: Default socket timeout at shutdown.
SHUTDOWN_SOCKET_TIMEOUT = 5.0

SELECT_UNKNOWN_QUEUE = """
Trying to select queue subset of {0!r}, but queue {1} isn't
defined in the `task_queues` setting.

If you want to automatically declare unknown queues you can
enable the `task_create_missing_queues` setting.
"""

DESELECT_UNKNOWN_QUEUE = """
Trying to deselect queue subset of {0!r}, but queue {1} isn't
defined in the `task_queues` setting.
"""

CSVListArgT = Union[Sequence[str], str]


class WorkController:
    """Unmanaged worker instance."""

    app: AppT = None

    pidlock: Pidfile = None
    blueprint: BlueprintT = None
    pool: PoolT = None
    semaphore: Any = None

    #: contains the exit code if a :exc:`SystemExit` event is handled.
    exitcode: int = None

    class Blueprint(bootsteps.Blueprint):
        """Worker bootstep blueprint."""

        name = 'Worker'
        default_steps: Set[Union[str, StepT]] = {
            'celery.worker.components:Hub',
            'celery.worker.components:Pool',
            'celery.worker.components:Beat',
            'celery.worker.components:Timer',
            'celery.worker.components:StateDB',
            'celery.worker.components:Consumer',
            'celery.worker.autoscale:WorkerComponent',
        }

    def __init__(
            self,
            *,
            app: AppT = None,
            hostname: str = None,
            **kwargs) -> None:
        self.app = app or self.app
        self.hostname = default_nodename(hostname)
        self.app.loader.init_worker()
        self.on_before_init(**kwargs)
        self.setup_defaults(**kwargs)
        self.on_after_init(**kwargs)

        self.setup_instance(**self.prepare_args(**kwargs))

    def setup_instance(self,
                       *,
                       queues: CSVListArgT = None,
                       ready_callback: Callable = None,
                       pidfile: str = None,
                       include: CSVListArgT = None,
                       use_eventloop: bool = None,
                       exclude_queues: CSVListArgT = None,
                       **kwargs) -> None:
        self.pidfile = pidfile
        self.setup_queues(queues, exclude_queues)
        self.setup_includes(str_to_list(include))

        # Set default concurrency
        if not self.concurrency:
            try:
                self.concurrency = cpu_count()
            except NotImplementedError:
                self.concurrency = 2

        # Options
        self.loglevel = mlevel(self.loglevel)
        self.ready_callback = ready_callback or self.on_consumer_ready

        # this connection won't establish, only used for params
        self._conninfo = self.app.connection_for_read()
        self.use_eventloop = (
            self.should_use_eventloop() if use_eventloop is None
            else use_eventloop
        )
        self.options = kwargs

        signals.worker_init.send(sender=self)

        # Initialize bootsteps
        self.pool_cls = _concurrency.get_implementation(self.pool_cls)
        self.steps = []
        self.on_init_blueprint()
        self.blueprint = self.Blueprint(
            steps=self.app.steps['worker'],
            on_start=self.on_start,
            on_close=self.on_close,
            on_stopped=self.on_stopped,
        )
        self.blueprint.apply(self, **kwargs)

    def on_init_blueprint(self) -> None:
        ...

    def on_before_init(self, **kwargs) -> None:
        ...

    def on_after_init(self, **kwargs) -> None:
        ...

    def on_start(self) -> None:
        if self.pidfile:
            self.pidlock = create_pidlock(self.pidfile)

    def on_consumer_ready(self, consumer: WorkerConsumerT) -> None:
        ...

    def on_close(self) -> None:
        self.app.loader.shutdown_worker()

    def on_stopped(self) -> None:
        self.timer.stop()
        self.consumer.shutdown()

        if self.pidlock:
            self.pidlock.release()

    def setup_queues(self, include: CSVListArgT,
                     exclude: CSVListArgT = None) -> None:
        include = str_to_list(include)
        exclude = str_to_list(exclude)
        try:
            self.app.amqp.queues.select(include)
        except KeyError as exc:
            raise ImproperlyConfigured(
                SELECT_UNKNOWN_QUEUE.strip().format(include, exc))
        try:
            self.app.amqp.queues.deselect(exclude)
        except KeyError as exc:
            raise ImproperlyConfigured(
                DESELECT_UNKNOWN_QUEUE.strip().format(exclude, exc))
        if self.app.conf.worker_direct:
            self.app.amqp.queues.select_add(worker_direct(self.hostname))

    def setup_includes(self, includes: Sequence[str]) -> None:
        # Update celery_include to have all known task modules, so that we
        # ensure all task modules are imported in case an execv happens.
        prev = tuple(self.app.conf.include)
        if includes:
            prev += tuple(includes)
            [self.app.loader.import_task_module(m) for m in includes]
        self.include = includes
        task_modules = {task.__class__.__module__
                        for task in self.app.tasks.values()}
        self.app.conf.include = tuple(set(prev) | task_modules)

    def prepare_args(self, **kwargs) -> Mapping:
        return kwargs

    def _send_worker_shutdown(self) -> None:
        signals.worker_shutdown.send(sender=self)

    async def start(self) -> None:
        try:
            await self.blueprint.start(self)
        except WorkerTerminate:
            await self.terminate()
        except Exception as exc:
            logger.critical('Unrecoverable error: %r', exc, exc_info=True)
            await self.stop(exitcode=EX_FAILURE)
        except SystemExit as exc:
            await self.stop(exitcode=exc.code)
        except KeyboardInterrupt:
            await self.stop(exitcode=EX_FAILURE)

    async def register_with_event_loop(self, hub: LoopT) -> None:
        await self.blueprint.send_all(
            self, 'register_with_event_loop', args=(hub,),
            description='hub.register',
        )

    async def _process_task_sem(self, req: RequestT) -> None:
        await self._quick_acquire(self._process_task, req)

    async def _process_task(self, req: RequestT) -> None:
        """Process task by sending it to the pool of workers."""
        try:
            await req.execute_using_pool(self.pool)
        except TaskRevokedError:
            try:
                self._quick_release()   # Issue 877
            except AttributeError:
                pass

    def signal_consumer_close(self) -> None:
        try:
            self.consumer.close()
        except AttributeError:
            pass

    def should_use_eventloop(self) -> bool:
        return (detect_environment() == 'default' and
                self._conninfo.transport.implements.async and
                not self.app.IS_WINDOWS)

    async def stop(self,
                   *,
                   in_sighandler: bool = False,
                   exitcode: int = None) -> None:
        """Graceful shutdown of the worker server."""
        if exitcode is not None:
            self.exitcode = exitcode
        if self.blueprint.state == RUN:
            self.signal_consumer_close()
            if not in_sighandler or self.pool.signal_safe:
                await self._shutdown(warm=True)
        self._send_worker_shutdown()

    async def terminate(self, *, in_sighandler: bool = False) -> None:
        """Not so graceful shutdown of the worker server."""
        if self.blueprint.state != TERMINATE:
            self.signal_consumer_close()
            if not in_sighandler or self.pool.signal_safe:
                await self._shutdown(warm=False)

    async def _shutdown(self, *, warm: bool = True) -> None:
        # if blueprint does not exist it means that we had an
        # error before the bootsteps could be initialized.
        if self.blueprint is not None:
            with default_socket_timeout(SHUTDOWN_SOCKET_TIMEOUT):  # Issue 975
                await self.blueprint.stop(self, terminate=not warm)
                self.blueprint.join()

    def reload(self,
               modules: Sequence[str] = None,
               *,
               reload: bool = False,
               reloader: Callable = None) -> None:
        list(self._reload_modules(
            modules, force_reload=reload, reloader=reloader))

        if self.consumer:
            self.consumer.update_strategies()
            self.consumer.reset_rate_limits()
        try:
            self.pool.restart()
        except NotImplementedError:
            pass

    def _reload_modules(self, modules: Sequence[str] = None, **kwargs) -> None:
        return (
            self._maybe_reload_module(m, **kwargs)
            for m in set(self.app.loader.task_modules
                         if modules is None else (modules or ()))
        )

    def _maybe_reload_module(self, module: str,
                             *,
                             force_reload: bool = False,
                             reloader: Callable = None) -> Any:
        if module not in sys.modules:
            logger.debug('importing module %s', module)
            return self.app.loader.import_from_cwd(module)
        elif force_reload:
            logger.debug('reloading module %s', module)
            return reload_from_cwd(sys.modules[module], reloader)

    def info(self) -> Mapping[str, Any]:
        return {'total': self.state.total_count,
                'pid': os.getpid(),
                'clock': str(self.app.clock)}

    def rusage(self) -> Mapping[str, Any]:
        if resource is None:
            raise NotImplementedError('rusage not supported by this platform')
        s = resource.getrusage(resource.RUSAGE_SELF)
        return {
            'utime': s.ru_utime,
            'stime': s.ru_stime,
            'maxrss': s.ru_maxrss,
            'ixrss': s.ru_ixrss,
            'idrss': s.ru_idrss,
            'isrss': s.ru_isrss,
            'minflt': s.ru_minflt,
            'majflt': s.ru_majflt,
            'nswap': s.ru_nswap,
            'inblock': s.ru_inblock,
            'oublock': s.ru_oublock,
            'msgsnd': s.ru_msgsnd,
            'msgrcv': s.ru_msgrcv,
            'nsignals': s.ru_nsignals,
            'nvcsw': s.ru_nvcsw,
            'nivcsw': s.ru_nivcsw,
        }

    def stats(self) -> Mapping[str, Any]:
        info = self.info()
        info.update(self.blueprint.info(self))
        info.update(self.consumer.blueprint.info(self.consumer))
        try:
            info['rusage'] = self.rusage()
        except NotImplementedError:
            info['rusage'] = 'N/A'
        return info

    def __repr__(self) -> str:
        """``repr(worker)``."""
        return '<Worker: {self.hostname} ({state})>'.format(
            self=self,
            state=self.blueprint.human_state() if self.blueprint else 'INIT',
        )

    def __str__(self) -> str:
        """``str(worker) == worker.hostname``."""
        return self.hostname

    @property
    def state(self) -> int:
        return state

    def setup_defaults(self,
                       *,
                       concurrency: int = None,
                       loglevel: Union[str, int] = 'WARN',
                       logfile: str = None,
                       task_events: bool = None,
                       pool: Union[str, type] = None,
                       consumer_cls: Union[str, type] = None,
                       timer_cls: Union[str, type] = None,
                       timer_precision: float = None,
                       autoscaler_cls: Union[str, type] = None,
                       pool_putlocks: bool = None,
                       pool_restarts: bool = None,
                       optimization: str = None,
                       O: str = None,  # O maps to -O=fair
                       statedb: str = None,
                       time_limit: float = None,
                       soft_time_limit: float = None,
                       scheduler: Union[str, type] = None,
                       schedule_filename: str = None,
                       max_tasks_per_child: int = None,
                       prefetch_multiplier: float = None,
                       disable_rate_limits: bool = None,
                       worker_lost_wait: float = None,
                       max_memory_per_child: float = None,
                       **_kw) -> None:
        either = self.app.either
        self.loglevel = loglevel
        self.logfile = logfile

        self.concurrency = either('worker_concurrency', concurrency)
        self.task_events = either('worker_send_task_events', task_events)
        self.pool_cls = either('worker_pool', pool)
        self.consumer_cls = either('worker_consumer', consumer_cls)
        self.timer_cls = either('worker_timer', timer_cls)
        self.timer_precision = either(
            'worker_timer_precision', timer_precision,
        )
        self.optimization = optimization or O
        self.autoscaler_cls = either('worker_autoscaler', autoscaler_cls)
        self.pool_putlocks = either('worker_pool_putlocks', pool_putlocks)
        self.pool_restarts = either('worker_pool_restarts', pool_restarts)
        self.statedb = either('worker_state_db', statedb)
        self.schedule_filename = either(
            'beat_schedule_filename', schedule_filename,
        )
        self.scheduler = either('beat_scheduler', scheduler)
        self.time_limit = either('task_time_limit', time_limit)
        self.soft_time_limit = either('task_soft_time_limit', soft_time_limit)
        self.max_tasks_per_child = either(
            'worker_max_tasks_per_child', max_tasks_per_child,
        )
        self.max_memory_per_child = either(
            'worker_max_memory_per_child', max_memory_per_child,
        )
        self.prefetch_multiplier = int(either(
            'worker_prefetch_multiplier', prefetch_multiplier,
        ))
        self.disable_rate_limits = either(
            'worker_disable_rate_limits', disable_rate_limits,
        )
        self.worker_lost_wait = either('worker_lost_wait', worker_lost_wait)
