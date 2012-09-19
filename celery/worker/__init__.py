# -*- coding: utf-8 -*-
"""
    celery.worker
    ~~~~~~~~~~~~~

    :class:`WorkController` can be used to instantiate in-process workers.

    The worker consists of several components, all managed by boot-steps
    (mod:`celery.worker.bootsteps`).

"""
from __future__ import absolute_import

import socket
import sys
import traceback

from threading import Event

from billiard import cpu_count
from kombu.syn import detect_environment
from kombu.utils.finalize import Finalize

from celery import concurrency as _concurrency
from celery import platforms
from celery import signals
from celery.app import app_or_default
from celery.app.abstract import configurated, from_config
from celery.exceptions import (
    ImproperlyConfigured, SystemTerminate, TaskRevokedError,
)
from celery.utils import worker_direct
from celery.utils.imports import qualname, reload_from_cwd
from celery.utils.log import mlevel, worker_logger as logger

from . import bootsteps
from . import state

try:
    from greenlet import GreenletExit
    IGNORE_ERRORS = (GreenletExit, )
except ImportError:  # pragma: no cover
    IGNORE_ERRORS = ()

#: Worker states
RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

UNKNOWN_QUEUE = """\
Trying to select queue subset of {0!r}, but queue {1} is not
defined in the CELERY_QUEUES setting.

If you want to automatically declare unknown queues you can
enable the CELERY_CREATE_MISSING_QUEUES setting.
"""

#: Default socket timeout at shutdown.
SHUTDOWN_SOCKET_TIMEOUT = 5.0


class Namespace(bootsteps.Namespace):
    """This is the boot-step namespace of the :class:`WorkController`.

    It loads modules from :setting:`CELERYD_BOOT_STEPS`, and its
    own set of built-in boot-step modules.

    """
    name = 'worker'
    builtin_boot_steps = ('celery.worker.components',
                          'celery.worker.autoscale',
                          'celery.worker.autoreload',
                          'celery.worker.consumer',
                          'celery.worker.mediator')

    def modules(self):
        return self.builtin_boot_steps + self.app.conf.CELERYD_BOOT_STEPS


class WorkController(configurated):
    """Unmanaged worker instance."""
    RUN = RUN
    CLOSE = CLOSE
    TERMINATE = TERMINATE

    app = None
    concurrency = from_config()
    loglevel = from_config('log_level')
    logfile = from_config('log_file')
    send_events = from_config()
    pool_cls = from_config('pool')
    consumer_cls = from_config('consumer')
    mediator_cls = from_config('mediator')
    timer_cls = from_config('timer')
    timer_precision = from_config('timer_precision')
    autoscaler_cls = from_config('autoscaler')
    autoreloader_cls = from_config('autoreloader')
    schedule_filename = from_config()
    scheduler_cls = from_config('celerybeat_scheduler')
    task_time_limit = from_config()
    task_soft_time_limit = from_config()
    max_tasks_per_child = from_config()
    pool_putlocks = from_config()
    pool_restarts = from_config()
    force_execv = from_config()
    prefetch_multiplier = from_config()
    state_db = from_config()
    disable_rate_limits = from_config()
    worker_lost_wait = from_config()

    _state = None
    _running = 0
    pidlock = None

    def __init__(self, app=None, hostname=None, **kwargs):
        self.app = app_or_default(app or self.app)
        self.hostname = hostname or socket.gethostname()
        self.on_before_init(**kwargs)

        self._finalize = Finalize(self, self.stop, exitpriority=1)
        self._shutdown_complete = Event()
        self.setup_instance(**self.prepare_args(**kwargs))

    def on_before_init(self, **kwargs):
        pass

    def on_start(self):
        pass

    def on_consumer_ready(self, consumer):
        pass

    def setup_instance(self, queues=None, ready_callback=None,
            pidfile=None, include=None, **kwargs):
        self.pidfile = pidfile
        self.app.loader.init_worker()
        self.setup_defaults(kwargs, namespace='celeryd')
        self.setup_queues(queues)
        self.setup_includes(include)

        # Set default concurrency
        if not self.concurrency:
            try:
                self.concurrency = cpu_count()
            except NotImplementedError:
                self.concurrency = 2

        # Options
        self.loglevel = mlevel(self.loglevel)
        self.ready_callback = ready_callback or self.on_consumer_ready
        self.use_eventloop = self.should_use_eventloop()

        signals.worker_init.send(sender=self)

        # Initialize boot steps
        self.pool_cls = _concurrency.get_implementation(self.pool_cls)
        self.components = []
        self.namespace = Namespace(app=self.app).apply(self, **kwargs)

    def setup_queues(self, queues):
        if isinstance(queues, basestring):
            queues = queues.split(',')
        self.queues = queues
        try:
            self.app.select_queues(queues)
        except KeyError as exc:
            raise ImproperlyConfigured(
                    UNKNOWN_QUEUE.format(queues, exc))
        if self.app.conf.CELERY_WORKER_DIRECT:
            self.app.amqp.queues.select_add(worker_direct(self.hostname))

    def setup_includes(self, includes):
        # Update celery_include to have all known task modules, so that we
        # ensure all task modules are imported in case an execv happens.
        inc = self.app.conf.CELERY_INCLUDE
        if includes:
            if isinstance(includes, basestring):
                includes = includes.split(',')
            inc = self.app.conf.CELERY_INCLUDE = tuple(inc) + tuple(includes)
        self.include = includes
        task_modules = set(task.__class__.__module__
                            for task in self.app.tasks.itervalues())
        self.app.conf.CELERY_INCLUDE = tuple(set(inc) | task_modules)

    def prepare_args(self, **kwargs):
        return kwargs

    def start(self):
        """Starts the workers main loop."""
        self.on_start()
        self._state = self.RUN
        if self.pidfile:
            self.pidlock = platforms.create_pidlock(self.pidfile)
        try:
            for i, component in enumerate(self.components):
                logger.debug('Starting %s...', qualname(component))
                self._running = i + 1
                if component:
                    component.start()
                logger.debug('%s OK!', qualname(component))
        except SystemTerminate:
            self.terminate()
        except Exception as exc:
            logger.error('Unrecoverable error: %r', exc,
                         exc_info=True)
            self.stop()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

        try:
            # Will only get here if running green,
            # makes sure all greenthreads have exited.
            self._shutdown_complete.wait()
        except IGNORE_ERRORS:
            pass
    run = start   # XXX Compat

    def process_task_sem(self, req):
        return self._quick_acquire(self.process_task, req)

    def process_task(self, req):
        """Process task by sending it to the pool of workers."""
        try:
            req.execute_using_pool(self.pool)
        except TaskRevokedError:
            try:
                self._quick_release()   # Issue 877
            except AttributeError:
                pass
        except Exception as exc:
            logger.critical('Internal error: %r\n%s',
                            exc, traceback.format_exc(), exc_info=True)
        except SystemTerminate:
            self.terminate()
            raise
        except BaseException as exc:
            self.stop()
            raise exc

    def signal_consumer_close(self):
        try:
            self.consumer.close()
        except AttributeError:
            pass

    def should_use_eventloop(self):
        return (detect_environment() == 'default' and
                self.app.connection().is_evented and not self.app.IS_WINDOWS)

    def stop(self, in_sighandler=False):
        """Graceful shutdown of the worker server."""
        self.signal_consumer_close()
        if not in_sighandler or self.pool.signal_safe:
            self._shutdown(warm=True)

    def terminate(self, in_sighandler=False):
        """Not so graceful shutdown of the worker server."""
        self.signal_consumer_close()
        if not in_sighandler or self.pool.signal_safe:
            self._shutdown(warm=False)

    def _shutdown(self, warm=True):
        what = 'Stopping' if warm else 'Terminating'
        socket_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(SHUTDOWN_SOCKET_TIMEOUT)  # Issue 975

        if self._state in (self.CLOSE, self.TERMINATE):
            return

        self.app.loader.shutdown_worker()

        if self.pool:
            self.pool.close()

        if self._state != self.RUN or self._running != len(self.components):
            # Not fully started, can safely exit.
            self._state = self.TERMINATE
            self._shutdown_complete.set()
            return
        self._state = self.CLOSE

        for component in reversed(self.components):
            logger.debug('%s %s...', what, qualname(component))
            if component:
                stop = component.stop
                if not warm:
                    stop = getattr(component, 'terminate', None) or stop
                stop()

        self.timer.stop()
        self.consumer.close_connection()

        if self.pidlock:
            self.pidlock.release()
        self._state = self.TERMINATE
        socket.setdefaulttimeout(socket_timeout)
        self._shutdown_complete.set()

    def reload(self, modules=None, reload=False, reloader=None):
        modules = self.app.loader.task_modules if modules is None else modules
        imp = self.app.loader.import_from_cwd

        for module in set(modules or ()):
            if module not in sys.modules:
                logger.debug('importing module %s', module)
                imp(module)
            elif reload:
                logger.debug('reloading module %s', module)
                reload_from_cwd(sys.modules[module], reloader)
        self.pool.restart()

    @property
    def state(self):
        return state
