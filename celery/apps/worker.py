# -*- coding: utf-8 -*-
"""
    celery.apps.worker
    ~~~~~~~~~~~~~~~~~~

    This module is the 'program-version' of :mod:`celery.worker`.

    It does everything necessary to run that module
    as an actual application, like installing signal handlers,
    platform tweaks, and so on.

"""
from __future__ import absolute_import

import logging
import os
import platform as _platform
import socket
import sys
import warnings

from functools import partial

from billiard import cpu_count, current_process
from kombu.utils.encoding import safe_str

from celery import VERSION_BANNER, platforms, signals
from celery.app import app_or_default
from celery.app.abstract import configurated, from_config
from celery.exceptions import ImproperlyConfigured, SystemTerminate
from celery.loaders.app import AppLoader
from celery.task import trace
from celery.utils import cry, isatty, worker_direct
from celery.utils.imports import qualname
from celery.utils.log import get_logger, mlevel, set_in_sighandler
from celery.utils.text import pluralize
from celery.worker import WorkController

try:
    from greenlet import GreenletExit
    IGNORE_ERRORS = (GreenletExit, )
except ImportError:  # pragma: no cover
    IGNORE_ERRORS = ()

logger = get_logger(__name__)
is_jython = sys.platform.startswith('java')
is_pypy = hasattr(sys, 'pypy_version_info')


def active_thread_count():
    from threading import enumerate
    # must use .getName on Python 2.5
    return sum(1 for t in enumerate()
               if not t.getName().startswith('Dummy-'))


def safe_say(msg):
    sys.__stderr__.write('\n%s\n' % msg)

ARTLINES = [
    ' --------------',
    '---- **** -----',
    '--- * ***  * --',
    '-- * - **** ---',
    '- ** ----------',
    '- ** ----------',
    '- ** ----------',
    '- ** ----------',
    '- *** --- * ---',
    '-- ******* ----',
    '--- ***** -----',
    ' --------------',
]

BANNER = """\
celery@%(hostname)s v%(version)s

%(platform)s

[config]
.> broker:      %(conninfo)s
.> app:         %(app)s
.> concurrency: %(concurrency)s
.> events:      %(events)s

[queues]
%(queues)s
"""

EXTRA_INFO_FMT = """
[Tasks]
%(tasks)s
"""

UNKNOWN_QUEUE = """\
Trying to select queue subset of %r, but queue %s is not
defined in the CELERY_QUEUES setting.

If you want to automatically declare unknown queues you can
enable the CELERY_CREATE_MISSING_QUEUES setting.
"""


class Worker(configurated):
    WorkController = WorkController

    app = None
    inherit_confopts = (WorkController, )
    loglevel = from_config('log_level')
    redirect_stdouts = from_config()
    redirect_stdouts_level = from_config()

    def __init__(self, hostname=None, purge=False, beat=False,
                 queues=None, include=None, app=None, pidfile=None,
                 autoscale=None, autoreload=False, no_execv=False,
                 no_color=None, **kwargs):
        self.app = app = app_or_default(app or self.app)
        self.hostname = hostname or socket.gethostname()

        # this signal can be used to set up configuration for
        # workers by name.
        signals.celeryd_init.send(sender=self.hostname, instance=self,
                                  conf=self.app.conf)

        self.setup_defaults(kwargs, namespace='celeryd')
        if not self.concurrency:
            try:
                self.concurrency = cpu_count()
            except NotImplementedError:
                self.concurrency = 2
        self.purge = purge
        self.beat = beat
        self.use_queues = [] if queues is None else queues
        self.queues = None
        self.include = include
        self.pidfile = pidfile
        self.autoscale = None
        self.autoreload = autoreload
        self.no_color = no_color
        self.no_execv = no_execv
        if autoscale:
            max_c, _, min_c = autoscale.partition(',')
            self.autoscale = [int(max_c), min_c and int(min_c) or 0]
        self._isatty = isatty(sys.stdout)

        self.colored = app.log.colored(
            self.logfile,
            enabled=not no_color if no_color is not None else no_color
        )

        if isinstance(self.use_queues, basestring):
            self.use_queues = self.use_queues.split(',')
        if self.include:
            if isinstance(self.include, basestring):
                self.include = self.include.split(',')
            app.conf.CELERY_INCLUDE = (
                tuple(app.conf.CELERY_INCLUDE) + tuple(self.include))
        self.loglevel = mlevel(self.loglevel)

    def run(self):
        self.init_queues()
        self.app.loader.init_worker()

        # this signal can be used to e.g. change queues after
        # the -Q option has been applied.
        signals.celeryd_after_setup.send(sender=self.hostname, instance=self,
                                         conf=self.app.conf)

        if getattr(os, 'getuid', None) and os.getuid() == 0:
            warnings.warn(RuntimeWarning(
                'Running celeryd with superuser privileges is discouraged!'))

        if self.purge:
            self.purge_messages()

        # Dump configuration to screen so we have some basic information
        # for when users sends bug reports.
        print(str(self.colored.cyan(' \n', self.startup_info())) +
              str(self.colored.reset(self.extra_info() or '')))
        self.set_process_status('-active-')

        self.setup_logging()

        # apply task execution optimizations
        trace.setup_worker_optimizations(self.app)

        try:
            self.run_worker()
        except IGNORE_ERRORS:
            pass

    def on_consumer_ready(self, consumer):
        signals.worker_ready.send(sender=consumer)
        print('celery@%s ready.' % safe_str(self.hostname))

    def init_queues(self):
        try:
            self.app.select_queues(self.use_queues)
        except KeyError, exc:
            raise ImproperlyConfigured(UNKNOWN_QUEUE % (self.use_queues, exc))
        if self.app.conf.CELERY_WORKER_DIRECT:
            self.app.amqp.queues.select_add(worker_direct(self.hostname))

    def setup_logging(self, colorize=None):
        if colorize is None and self.no_color is not None:
            colorize = not self.no_color
        self.app.log.setup(self.loglevel, self.logfile,
                           self.redirect_stdouts, self.redirect_stdouts_level,
                           colorize=colorize)

    def purge_messages(self):
        count = self.app.control.purge()
        print('purge: Erased %d %s from the queue.\n' % (
            count, pluralize(count, 'message')))

    def tasklist(self, include_builtins=True, sep='\n', int_='celery.'):
        return sep.join(
            '  . %s' % task for task in sorted(self.app.tasks)
            if (not task.startswith(int_) if not include_builtins else task)
        )

    def extra_info(self):
        if self.loglevel <= logging.INFO:
            include_builtins = self.loglevel <= logging.DEBUG
            tasklist = self.tasklist(include_builtins=include_builtins)
            return EXTRA_INFO_FMT % {'tasks': tasklist}

    def startup_info(self):
        app = self.app
        concurrency = unicode(self.concurrency)
        appr = '%s:0x%x' % (app.main or '__main__', id(app))
        if not isinstance(app.loader, AppLoader):
            loader = qualname(app.loader)
            if loader.startswith('celery.loaders'):
                loader = loader[14:]
            appr += ' (%s)' % loader
        if self.autoscale:
            max, min = self.autoscale
            concurrency = '{min=%s, max=%s}' % (min, max)
        pool = self.pool_cls
        if not isinstance(pool, basestring):
            pool = pool.__module__
        concurrency += ' (%s)' % pool.split('.')[-1]
        events = 'ON'
        if not self.send_events:
            events = 'OFF (enable -E to monitor this worker)'

        banner = (BANNER % {
            'app': appr,
            'hostname': self.hostname,
            'version': VERSION_BANNER,
            'conninfo': self.app.connection().as_uri(),
            'concurrency': concurrency,
            'platform': safe_str(_platform.platform()),
            'events': events,
            'queues': app.amqp.queues.format(indent=0, indent_first=False),
        }).splitlines()

        # integrate the ASCII art.
        for i, x in enumerate(banner):
            try:
                banner[i] = ' '.join([ARTLINES[i], banner[i]])
            except IndexError:
                banner[i] = ' ' * 16 + banner[i]
        return '\n'.join(banner) + '\n'

    def run_worker(self):
        worker = self.WorkController(
            app=self.app,
            hostname=self.hostname,
            ready_callback=self.on_consumer_ready, beat=self.beat,
            autoscale=self.autoscale, autoreload=self.autoreload,
            no_execv=self.no_execv,
            pidfile=self.pidfile,
            **self.confopts_as_dict()
        )
        self.install_platform_tweaks(worker)
        signals.worker_init.send(sender=worker)
        worker.start()

    def install_platform_tweaks(self, worker):
        """Install platform specific tweaks and workarounds."""
        if self.app.IS_OSX:
            self.osx_proxy_detection_workaround()

        # Install signal handler so SIGHUP restarts the worker.
        if not self._isatty:
            # only install HUP handler if detached from terminal,
            # so closing the terminal window doesn't restart celeryd
            # into the background.
            if self.app.IS_OSX:
                # OS X can't exec from a process using threads.
                # See http://github.com/celery/celery/issues#issue/152
                install_HUP_not_supported_handler(worker)
            else:
                install_worker_restart_handler(worker)
        install_worker_term_handler(worker)
        install_worker_term_hard_handler(worker)
        install_worker_int_handler(worker)
        install_cry_handler()
        install_rdb_handler()

    def osx_proxy_detection_workaround(self):
        """See http://github.com/celery/celery/issues#issue/161"""
        os.environ.setdefault('celery_dummy_proxy', 'set_by_celeryd')

    def set_process_status(self, info):
        return platforms.set_mp_process_title(
            'celeryd',
            info='%s (%s)' % (info, platforms.strargv(sys.argv)),
            hostname=self.hostname,
        )


def _shutdown_handler(worker, sig='TERM', how='Warm',
                      exc=SystemExit, callback=None):

    def _handle_request(*args):
        set_in_sighandler(True)
        try:
            from celery.worker import state
            if current_process()._name == 'MainProcess':
                if callback:
                    callback(worker)
                    safe_say('celeryd: %s shutdown (MainProcess)' % how)
            if active_thread_count() > 1:
                setattr(state, {'Warm': 'should_stop',
                                'Cold': 'should_terminate'}[how], True)
            else:
                raise exc()
        finally:
            set_in_sighandler(False)
    _handle_request.__name__ = 'worker_' + how
    platforms.signals[sig] = _handle_request
install_worker_term_handler = partial(
    _shutdown_handler, sig='SIGTERM', how='Warm', exc=SystemExit,
)
if not is_jython:
    install_worker_term_hard_handler = partial(
        _shutdown_handler, sig='SIGQUIT', how='Cold', exc=SystemTerminate,
    )
else:
    install_worker_term_handler = \
        install_worker_term_hard_handler = lambda *a, **kw: None


def on_SIGINT(worker):
    safe_say('celeryd: Hitting Ctrl+C again will terminate all running tasks!')
    install_worker_term_hard_handler(worker, sig='SIGINT')
if not is_jython:
    install_worker_int_handler = partial(
        _shutdown_handler, sig='SIGINT', callback=on_SIGINT
    )
else:
    install_worker_int_handler = lambda *a, **kw: None


def _clone_current_worker():
    if os.fork() == 0:
        platforms.close_open_fds([
            sys.__stdin__, sys.__stdout__, sys.__stderr__,
        ])
        os.execv(sys.executable, [sys.executable] + sys.argv)


def install_worker_restart_handler(worker, sig='SIGHUP'):

    def restart_worker_sig_handler(*args):
        """Signal handler restarting the current python program."""
        set_in_sighandler(True)
        safe_say('Restarting celeryd (%s)' % (' '.join(sys.argv), ))
        import atexit
        atexit.register(_clone_current_worker)
        from celery.worker import state
        state.should_stop = True
    platforms.signals[sig] = restart_worker_sig_handler


def install_cry_handler():
    # Jython/PyPy does not have sys._current_frames
    if is_jython or is_pypy:  # pragma: no cover
        return

    def cry_handler(*args):
        """Signal handler logging the stacktrace of all active threads."""
        set_in_sighandler(True)
        try:
            safe_say(cry())
        finally:
            set_in_sighandler(False)
    platforms.signals['SIGUSR1'] = cry_handler


def install_rdb_handler(envvar='CELERY_RDBSIG',
                        sig='SIGUSR2'):  # pragma: no cover

    def rdb_handler(*args):
        """Signal handler setting a rdb breakpoint at the current frame."""
        set_in_sighandler(True)
        try:
            _, frame = args
            from celery.contrib import rdb
            rdb.set_trace(frame)
        finally:
            set_in_sighandler(False)
    if os.environ.get(envvar):
        platforms.signals[sig] = rdb_handler


def install_HUP_not_supported_handler(worker, sig='SIGHUP'):

    def warn_on_HUP_handler(*args):
        set_in_sighandler(True)
        try:
            safe_say('%(sig)s not supported: Restarting with %(sig)s is '
                     'unstable on this platform!' % {'sig': sig})
        finally:
            set_in_sighandler(False)
    platforms.signals[sig] = warn_on_HUP_handler
