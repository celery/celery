# -*- coding: utf-8 -*-
"""
    celery.worker
    ~~~~~~~~~~~~~

    :class:`WorkController` can be used to instantiate in-process workers.

    The worker consists of several components, all managed by bootsteps
    (mod:`celery.bootsteps`).

"""
from __future__ import absolute_import

import os
import resource
import socket
import sys
import traceback

from billiard import cpu_count
from billiard.util import Finalize
from kombu.syn import detect_environment

from celery import bootsteps
from celery import concurrency as _concurrency
from celery import platforms
from celery import signals
from celery.app import app_or_default
from celery.app.abstract import configurated, from_config
from celery.exceptions import (
    ImproperlyConfigured, SystemTerminate, TaskRevokedError,
)
from celery.five import string_t, values
from celery.utils import nodename, nodesplit, worker_direct
from celery.utils.imports import reload_from_cwd
from celery.utils.log import mlevel, worker_logger as logger

from . import state

UNKNOWN_QUEUE = """\
Trying to select queue subset of {0!r}, but queue {1} is not
defined in the CELERY_QUEUES setting.

If you want to automatically declare unknown queues you can
enable the CELERY_CREATE_MISSING_QUEUES setting.
"""


def default_nodename(hostname):
    name, host = nodesplit(hostname or '')
    return nodename(name or 'celery', host or socket.gethostname())


class WorkController(configurated):
    """Unmanaged worker instance."""
    app = None
    concurrency = from_config()
    loglevel = from_config('log_level')
    logfile = from_config('log_file')
    send_events = from_config()
    pool_cls = from_config('pool')
    consumer_cls = from_config('consumer')
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

    pidlock = None
    namespace = None
    pool = None
    semaphore = None

    class Namespace(bootsteps.Namespace):
        """Worker bootstep namespace."""
        name = 'Worker'
        default_steps = set([
            'celery.worker.components:Hub',
            'celery.worker.components:Queues',
            'celery.worker.components:Pool',
            'celery.worker.components:Beat',
            'celery.worker.components:Timer',
            'celery.worker.components:StateDB',
            'celery.worker.components:Consumer',
            'celery.worker.autoscale:WorkerComponent',
            'celery.worker.autoreload:WorkerComponent',

        ])

    def __init__(self, app=None, hostname=None, **kwargs):
        self.app = app_or_default(app or self.app)
        self.hostname = default_nodename(hostname)
        self.on_before_init_worker(**kwargs)
        self.app.loader.init_worker()
        self.on_before_init(**kwargs)

        self._finalize = [
            Finalize(self, self.stop, exitpriority=1),
            Finalize(self, self._send_worker_shutdown, exitpriority=10),
        ]
        self.setup_instance(**self.prepare_args(**kwargs))

    def setup_instance(self, queues=None, ready_callback=None, pidfile=None,
                       include=None, use_eventloop=None, **kwargs):
        self.pidfile = pidfile
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

        # this connection is not established, only used for params
        self._conninfo = self.app.connection()
        self.use_eventloop = (
            self.should_use_eventloop() if use_eventloop is None
            else use_eventloop
        )
        self.options = kwargs

        signals.worker_init.send(sender=self)

        # Initialize bootsteps
        self.pool_cls = _concurrency.get_implementation(self.pool_cls)
        self.steps = []
        self.on_init_namespace()
        self.namespace = self.Namespace(app=self.app,
                                        on_start=self.on_start,
                                        on_close=self.on_close,
                                        on_stopped=self.on_stopped)
        self.namespace.apply(self, **kwargs)

    def on_init_namespace(self):
        pass

    def on_before_init_worker(self, **kwargs):
        pass

    def on_before_init(self, **kwargs):
        pass

    def on_start(self):
        if self.pidfile:
            self.pidlock = platforms.create_pidlock(self.pidfile)

    def on_consumer_ready(self, consumer):
        pass

    def on_close(self):
        self.app.loader.shutdown_worker()

    def on_stopped(self):
        self.timer.stop()
        self.consumer.shutdown()

        if self.pidlock:
            self.pidlock.release()

    def setup_queues(self, queues):
        if isinstance(queues, string_t):
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
            if isinstance(includes, string_t):
                includes = includes.split(',')
            inc = self.app.conf.CELERY_INCLUDE = tuple(inc) + tuple(includes)
        self.include = includes
        task_modules = set(task.__class__.__module__
                           for task in values(self.app.tasks))
        self.app.conf.CELERY_INCLUDE = tuple(set(inc) | task_modules)

    def prepare_args(self, **kwargs):
        return kwargs

    def _send_worker_shutdown(self):
        signals.worker_shutdown.send(sender=self)

    def start(self):
        """Starts the workers main loop."""
        try:
            self.namespace.start(self)
        except SystemTerminate:
            self.terminate()
        except Exception as exc:
            logger.error('Unrecoverable error: %r', exc, exc_info=True)
            self.stop()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def _process_task_sem(self, req):
        return self._quick_acquire(self._process_task, req)

    def _process_task(self, req):
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
                self._conninfo.is_evented and not self.app.IS_WINDOWS)

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
        # if namespace does not exist it means that we had an
        # error before the bootsteps could be initialized.
        if self.namespace is not None:
            self.namespace.stop(self, terminate=not warm)
            self.namespace.join()

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

    def info(self):
        return {'total': self.state.total_count,
                'pid': os.getpid(),
                'clock': str(self.app.clock)}

    def rusage(self):
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

    def stats(self):
        info = self.info()
        info.update(self.namespace.info(self))
        info.update(self.consumer.namespace.info(self.consumer))
        info.update(rusage=self.rusage())
        return info

    @property
    def _state(self):
        return self.namespace.state

    @property
    def state(self):
        return state
