# -*- coding: utf-8 -*-
"""
    celery.apps.worker
    ~~~~~~~~~~~~~~~~~~

    This module is the 'program-version' of :mod:`celery.worker`.

    It does everything necessary to run that module
    as an actual application, like installing signal handlers,
    platform tweaks, and so on.

"""
from __future__ import absolute_import, print_function

import logging
import os
import sys
import warnings

from functools import partial

from billiard import current_process
from kombu.utils.encoding import safe_str

from celery import VERSION_BANNER, platforms, signals
from celery.app.abstract import from_config
from celery.exceptions import SystemTerminate
from celery.five import string, string_t
from celery.loaders.app import AppLoader
from celery.task import trace
from celery.utils import cry, isatty
from celery.utils.imports import qualname
from celery.utils.log import get_logger, in_sighandler, set_in_sighandler
from celery.utils.text import pluralize
from celery.worker import WorkController

logger = get_logger(__name__)
is_jython = sys.platform.startswith('java')
is_pypy = hasattr(sys, 'pypy_version_info')


def active_thread_count():
    from threading import enumerate
    return sum(1 for t in enumerate()
               if not t.name.startswith('Dummy-'))


def safe_say(msg):
    print('\n{0}'.format(msg), file=sys.__stderr__)

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
{hostname} v{version}

[Configuration]
. broker:      {conninfo}
. app:         {app}
. concurrency: {concurrency}
. events:      {events}

[Queues]
{queues}
"""

EXTRA_INFO_FMT = """
[Tasks]
{tasks}
"""


class Worker(WorkController):
    redirect_stdouts = from_config()
    redirect_stdouts_level = from_config()

    def on_before_init(self, purge=False, no_color=None, **kwargs):
        # apply task execution optimizations
        trace.setup_worker_optimizations(self.app)

        # this signal can be used to set up configuration for
        # workers by name.
        conf = self.app.conf
        signals.celeryd_init.send(
            sender=self.hostname, instance=self, conf=conf,
        )
        self.purge = purge
        self.no_color = no_color
        self._isatty = isatty(sys.stdout)
        self.colored = self.app.log.colored(
            self.logfile,
            enabled=not no_color if no_color is not None else no_color
        )

    def on_init_namespace(self):
        self._custom_logging = self.setup_logging()
        # apply task execution optimizations
        trace.setup_worker_optimizations(self.app)

    def on_start(self):
        if not self._custom_logging and self.redirect_stdouts:
            self.app.log.redirect_stdouts(self.redirect_stdouts_level)

        WorkController.on_start(self)

        # this signal can be used to e.g. change queues after
        # the -Q option has been applied.
        signals.celeryd_after_setup.send(
            sender=self.hostname, instance=self, conf=self.app.conf,
        )

        if getattr(os, 'getuid', None) and os.getuid() == 0:
            warnings.warn(RuntimeWarning(
                'Running the worker with superuser privileges is discouraged!',
            ))

        if self.purge:
            self.purge_messages()

        # Dump configuration to screen so we have some basic information
        # for when users sends bug reports.
        sys.__stdout__.write(
            str(self.colored.cyan(' \n', self.startup_info())) +
            str(self.colored.reset(self.extra_info() or '')) + '\n'
        )
        self.set_process_status('-active-')
        self.install_platform_tweaks(self)

    def on_consumer_ready(self, consumer):
        signals.worker_ready.send(sender=consumer)
        print('{0} ready.'.format(safe_str(self.hostname), ))

    def setup_logging(self, colorize=None):
        if colorize is None and self.no_color is not None:
            colorize = not self.no_color
        return self.app.log.setup(
            self.loglevel, self.logfile,
            redirect_stdouts=False, colorize=colorize,
        )

    def purge_messages(self):
        count = self.app.control.purge()
        print('purge: Erased {0} {1} from the queue.\n'.format(
            count, pluralize(count, 'message')))

    def tasklist(self, include_builtins=True):
        tasks = self.app.tasks
        if not include_builtins:
            tasks = (t for t in tasks if not t.startswith('celery.'))
        return '\n'.join('  . {0}'.format(task) for task in sorted(tasks))

    def extra_info(self):
        if self.loglevel <= logging.INFO:
            include_builtins = self.loglevel <= logging.DEBUG
            tasklist = self.tasklist(include_builtins=include_builtins)
            return EXTRA_INFO_FMT.format(tasks=tasklist)

    def startup_info(self):
        app = self.app
        concurrency = string(self.concurrency)
        appr = '{0}:0x{1:x}'.format(app.main or '__main__', id(app))
        if not isinstance(app.loader, AppLoader):
            loader = qualname(app.loader)
            if loader.startswith('celery.loaders'):
                loader = loader[14:]
            appr += ' ({0})'.format(loader)
        if self.autoscale:
            max, min = self.autoscale
            concurrency = '{{min={0}, max={1}}}'.format(min, max)
        pool = self.pool_cls
        if not isinstance(pool, string_t):
            pool = pool.__module__
        concurrency += ' ({0})'.format(pool.split('.')[-1])
        events = 'ON'
        if not self.send_events:
            events = 'OFF (enable -E to monitor this worker)'

        banner = BANNER.format(
            app=appr,
            hostname=self.hostname,
            version=VERSION_BANNER,
            conninfo=self.app.connection().as_uri(),
            concurrency=concurrency,
            events=events,
            queues=app.amqp.queues.format(indent=0, indent_first=False),
        ).splitlines()

        # integrate the ASCII art.
        for i, x in enumerate(banner):
            try:
                banner[i] = ' '.join([ARTLINES[i], banner[i]])
            except IndexError:
                banner[i] = ' ' * 16 + banner[i]
        return '\n'.join(banner) + '\n'

    def install_platform_tweaks(self, worker):
        """Install platform specific tweaks and workarounds."""
        if self.app.IS_OSX:
            self.osx_proxy_detection_workaround()

        # Install signal handler so SIGHUP restarts the worker.
        if not self._isatty:
            # only install HUP handler if detached from terminal,
            # so closing the terminal window doesn't restart the worker
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
            info='{0} ({1})'.format(info, platforms.strargv(sys.argv)),
            hostname=self.hostname,
        )


def _shutdown_handler(worker, sig='TERM', how='Warm',
                      exc=SystemExit, callback=None):

    def _handle_request(signum, frame):
        with in_sighandler():
            from celery.worker import state
            if current_process()._name == 'MainProcess':
                if callback:
                    callback(worker)
                safe_say('worker: {0} shutdown (MainProcess)'.format(how))
            if active_thread_count() > 1:
                setattr(state, {'Warm': 'should_stop',
                                'Cold': 'should_terminate'}[how], True)
            else:
                raise exc()
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
    safe_say('worker: Hitting Ctrl+C again will terminate all running tasks!')
    install_worker_term_hard_handler(worker, sig='SIGINT')
if not is_jython:
    install_worker_int_handler = partial(
        _shutdown_handler, sig='SIGINT', callback=on_SIGINT
    )
else:
    install_worker_int_handler = lambda *a, **kw: None


def _clone_current_worker():
    if os.fork() == 0:
        os.execv(sys.executable, [sys.executable] + sys.argv)


def install_worker_restart_handler(worker, sig='SIGHUP'):

    def restart_worker_sig_handler(*args):
        """Signal handler restarting the current python program."""
        set_in_sighandler(True)
        safe_say('Restarting celery worker ({0})'.format(' '.join(sys.argv)))
        import atexit
        atexit.register(_clone_current_worker)
        from celery.worker import state
        state.should_stop = True
    platforms.signals[sig] = restart_worker_sig_handler


def install_cry_handler(sig='SIGUSR1'):
    # Jython/PyPy does not have sys._current_frames
    if is_jython or is_pypy:  # pragma: no cover
        return

    def cry_handler(*args):
        """Signal handler logging the stacktrace of all active threads."""
        with in_sighandler():
            safe_say(cry())
    platforms.signals[sig] = cry_handler


def install_rdb_handler(envvar='CELERY_RDBSIG',
                        sig='SIGUSR2'):  # pragma: no cover

    def rdb_handler(*args):
        """Signal handler setting a rdb breakpoint at the current frame."""
        with in_sighandler():
            _, frame = args
            from celery.contrib import rdb
            rdb.set_trace(frame)
    if os.environ.get(envvar):
        platforms.signals[sig] = rdb_handler


def install_HUP_not_supported_handler(worker, sig='SIGHUP'):

    def warn_on_HUP_handler(signum, frame):
        with in_sighandler():
            safe_say('{sig} not supported: Restarting with {sig} is '
                     'unstable on this platform!'.format(sig=sig))
    platforms.signals[sig] = warn_on_HUP_handler
