"""Worker command-line program.

This module is the 'program-version' of :mod:`celery.worker`.

It does everything necessary to run that module
as an actual application, like installing signal handlers,
platform tweaks, and so on.
"""
import logging
import os
import platform as _platform
import sys
from datetime import datetime
from functools import partial

from billiard.common import REMAP_SIGTERM
from billiard.process import current_process
from kombu.utils.encoding import safe_str

from celery import VERSION_BANNER, platforms, signals
from celery.app import trace
from celery.loaders.app import AppLoader
from celery.platforms import EX_FAILURE, EX_OK, check_privileges, isatty
from celery.utils import static, term
from celery.utils.debug import cry
from celery.utils.imports import qualname
from celery.utils.log import get_logger, in_sighandler, set_in_sighandler
from celery.utils.text import pluralize
from celery.worker import WorkController

__all__ = ('Worker',)

logger = get_logger(__name__)
is_jython = sys.platform.startswith('java')
is_pypy = hasattr(sys, 'pypy_version_info')

ARTLINES = [
    ' --------------',
    '--- ***** -----',
    '-- ******* ----',
    '- *** --- * ---',
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

{platform} {timestamp}

[config]
.> app:         {app}
.> transport:   {conninfo}
.> results:     {results}
.> concurrency: {concurrency}
.> task events: {events}

[queues]
{queues}
"""

EXTRA_INFO_FMT = """
[tasks]
{tasks}
"""


def active_thread_count():
    from threading import enumerate
    return sum(1 for t in enumerate()
               if not t.name.startswith('Dummy-'))


def safe_say(msg, f=sys.__stderr__):
    if hasattr(f, 'fileno') and f.fileno() is not None:
        os.write(f.fileno(), f'\n{msg}\n'.encode())


class Worker(WorkController):
    """Worker as a program."""

    def on_before_init(self, quiet=False, **kwargs):
        self.quiet = quiet
        trace.setup_worker_optimizations(self.app, self.hostname)

        # this signal can be used to set up configuration for
        # workers by name.
        signals.celeryd_init.send(
            sender=self.hostname, instance=self,
            conf=self.app.conf, options=kwargs,
        )
        check_privileges(self.app.conf.accept_content)

    def on_after_init(self, purge=False, no_color=None,
                      redirect_stdouts=None, redirect_stdouts_level=None,
                      **kwargs):
        self.redirect_stdouts = self.app.either(
            'worker_redirect_stdouts', redirect_stdouts)
        self.redirect_stdouts_level = self.app.either(
            'worker_redirect_stdouts_level', redirect_stdouts_level)
        super().setup_defaults(**kwargs)
        self.purge = purge
        self.no_color = no_color
        self._isatty = isatty(sys.stdout)
        self.colored = self.app.log.colored(
            self.logfile,
            enabled=not no_color if no_color is not None else no_color
        )

    def on_init_blueprint(self):
        self._custom_logging = self.setup_logging()
        # apply task execution optimizations
        # -- This will finalize the app!
        trace.setup_worker_optimizations(self.app, self.hostname)

    def on_start(self):
        app = self.app
        super().on_start()

        # this signal can be used to, for example, change queues after
        # the -Q option has been applied.
        signals.celeryd_after_setup.send(
            sender=self.hostname, instance=self, conf=app.conf,
        )

        if self.purge:
            self.purge_messages()

        if not self.quiet:
            self.emit_banner()

        self.set_process_status('-active-')
        self.install_platform_tweaks(self)
        if not self._custom_logging and self.redirect_stdouts:
            app.log.redirect_stdouts(self.redirect_stdouts_level)

        # TODO: Remove the following code in Celery 6.0
        # This qualifies as a hack for issue #6366.
        warn_deprecated = True
        config_source = app._config_source
        if isinstance(config_source, str):
            # Don't raise the warning when the settings originate from
            # django.conf:settings
            warn_deprecated = config_source.lower() not in [
                'django.conf:settings',
            ]

        if warn_deprecated:
            if app.conf.maybe_warn_deprecated_settings():
                logger.warning(
                    "Please run `celery upgrade settings path/to/settings.py` "
                    "to avoid these warnings and to allow a smoother upgrade "
                    "to Celery 6.0."
                )

    def emit_banner(self):
        # Dump configuration to screen so we have some basic information
        # for when users sends bug reports.
        use_image = term.supports_images()
        if use_image:
            print(term.imgcat(static.logo()))
        print(safe_str(''.join([
            str(self.colored.cyan(
                ' \n', self.startup_info(artlines=not use_image))),
            str(self.colored.reset(self.extra_info() or '')),
        ])), file=sys.__stdout__, flush=True)

    def on_consumer_ready(self, consumer):
        signals.worker_ready.send(sender=consumer)
        logger.info('%s ready.', safe_str(self.hostname))

    def setup_logging(self, colorize=None):
        if colorize is None and self.no_color is not None:
            colorize = not self.no_color
        return self.app.log.setup(
            self.loglevel, self.logfile,
            redirect_stdouts=False, colorize=colorize, hostname=self.hostname,
        )

    def purge_messages(self):
        with self.app.connection_for_write() as connection:
            count = self.app.control.purge(connection=connection)
            if count:  # pragma: no cover
                print(f"purge: Erased {count} {pluralize(count, 'message')} from the queue.\n", flush=True)

    def tasklist(self, include_builtins=True, sep='\n', int_='celery.'):
        return sep.join(
            f'  . {task}' for task in sorted(self.app.tasks)
            if (not task.startswith(int_) if not include_builtins else task)
        )

    def extra_info(self):
        if self.loglevel is None:
            return
        if self.loglevel <= logging.INFO:
            include_builtins = self.loglevel <= logging.DEBUG
            tasklist = self.tasklist(include_builtins=include_builtins)
            return EXTRA_INFO_FMT.format(tasks=tasklist)

    def startup_info(self, artlines=True):
        app = self.app
        concurrency = str(self.concurrency)
        appr = '{}:{:#x}'.format(app.main or '__main__', id(app))
        if not isinstance(app.loader, AppLoader):
            loader = qualname(app.loader)
            if loader.startswith('celery.loaders'):  # pragma: no cover
                loader = loader[14:]
            appr += f' ({loader})'
        if self.autoscale:
            max, min = self.autoscale
            concurrency = f'{{min={min}, max={max}}}'
        pool = self.pool_cls
        if not isinstance(pool, str):
            pool = pool.__module__
        concurrency += f" ({pool.split('.')[-1]})"
        events = 'ON'
        if not self.task_events:
            events = 'OFF (enable -E to monitor tasks in this worker)'

        banner = BANNER.format(
            app=appr,
            hostname=safe_str(self.hostname),
            timestamp=datetime.now().replace(microsecond=0),
            version=VERSION_BANNER,
            conninfo=self.app.connection().as_uri(),
            results=self.app.backend.as_uri(),
            concurrency=concurrency,
            platform=safe_str(_platform.platform()),
            events=events,
            queues=app.amqp.queues.format(indent=0, indent_first=False),
        ).splitlines()

        # integrate the ASCII art.
        if artlines:
            for i, _ in enumerate(banner):
                try:
                    banner[i] = ' '.join([ARTLINES[i], banner[i]])
                except IndexError:
                    banner[i] = ' ' * 16 + banner[i]
        return '\n'.join(banner) + '\n'

    def install_platform_tweaks(self, worker):
        """Install platform specific tweaks and workarounds."""
        if self.app.IS_macOS:
            self.macOS_proxy_detection_workaround()

        # Install signal handler so SIGHUP restarts the worker.
        if not self._isatty:
            # only install HUP handler if detached from terminal,
            # so closing the terminal window doesn't restart the worker
            # into the background.
            if self.app.IS_macOS:
                # macOS can't exec from a process using threads.
                # See https://github.com/celery/celery/issues#issue/152
                install_HUP_not_supported_handler(worker)
            else:
                install_worker_restart_handler(worker)
        install_worker_term_handler(worker)
        install_worker_term_hard_handler(worker)
        install_worker_int_handler(worker)
        install_cry_handler()
        install_rdb_handler()

    def macOS_proxy_detection_workaround(self):
        """See https://github.com/celery/celery/issues#issue/161."""
        os.environ.setdefault('celery_dummy_proxy', 'set_by_celeryd')

    def set_process_status(self, info):
        return platforms.set_mp_process_title(
            'celeryd',
            info=f'{info} ({platforms.strargv(sys.argv)})',
            hostname=self.hostname,
        )


def _shutdown_handler(worker: Worker, sig='SIGTERM', how='Warm', callback=None, exitcode=EX_OK, verbose=True):
    """Install signal handler for warm/cold shutdown.

    The handler will run from the MainProcess.

    Args:
        worker (Worker): The worker that received the signal.
        sig (str, optional): The signal that was received. Defaults to 'TERM'.
        how (str, optional): The type of shutdown to perform. Defaults to 'Warm'.
        callback (Callable, optional): Signal handler. Defaults to None.
        exitcode (int, optional): The exit code to use. Defaults to EX_OK.
        verbose (bool, optional): Whether to print the type of shutdown. Defaults to True.
    """
    def _handle_request(*args):
        with in_sighandler():
            from celery.worker import state
            if current_process()._name == 'MainProcess':
                if callback:
                    callback(worker)
                if verbose:
                    safe_say(f'worker: {how} shutdown (MainProcess)', sys.__stdout__)
                signals.worker_shutting_down.send(
                    sender=worker.hostname, sig=sig, how=how,
                    exitcode=exitcode,
                )
            setattr(state, {'Warm': 'should_stop',
                            'Cold': 'should_terminate'}[how], exitcode)
    _handle_request.__name__ = str(f'worker_{how}')
    platforms.signals[sig] = _handle_request


def on_hard_shutdown(worker: Worker):
    """Signal handler for hard shutdown.

    The handler will terminate the worker immediately by force using the exit code ``EX_FAILURE``.

    In practice, you should never get here, as the standard shutdown process should be enough.
    This handler is only for the worst-case scenario, where the worker is stuck and cannot be
    terminated gracefully (e.g., spamming the Ctrl+C in the terminal to force the worker to terminate).

    Args:
        worker (Worker): The worker that received the signal.

    Raises:
        WorkerTerminate: This exception will be raised in the MainProcess to terminate the worker immediately.
    """
    from celery.exceptions import WorkerTerminate
    raise WorkerTerminate(EX_FAILURE)


def during_soft_shutdown(worker: Worker):
    """This signal handler is called when the worker is in the middle of the soft shutdown process.

    When the worker is in the soft shutdown process, it is waiting for tasks to finish. If the worker
    receives a SIGINT (Ctrl+C) or SIGQUIT signal (or possibly SIGTERM if REMAP_SIGTERM is set to "SIGQUIT"),
    the handler will cancels all unacked requests to allow the worker to terminate gracefully and replace the
    signal handler for SIGINT and SIGQUIT with the hard shutdown handler ``on_hard_shutdown`` to terminate
    the worker immediately by force next time the signal is received.

    It will give the worker once last chance to gracefully terminate (the cold shutdown), after canceling all
    unacked requests, before using the hard shutdown handler to terminate the worker forcefully.

    Args:
        worker (Worker): The worker that received the signal.
    """
    # Replace the signal handler for SIGINT (Ctrl+C) and SIGQUIT (and possibly SIGTERM)
    # with the hard shutdown handler to terminate the worker immediately by force
    install_worker_term_hard_handler(worker, sig='SIGINT', callback=on_hard_shutdown, verbose=False)
    install_worker_term_hard_handler(worker, sig='SIGQUIT', callback=on_hard_shutdown)

    # Cancel all unacked requests and allow the worker to terminate naturally
    worker.consumer.cancel_all_unacked_requests()

    # We get here if the worker was in the middle of the soft (cold) shutdown process,
    # and the matching signal was received. This can typically happen when the worker is
    # waiting for tasks to finish, and the user decides to still cancel the running tasks.
    # We give the worker the last chance to gracefully terminate by letting the soft shutdown
    # waiting time to finish, which is running in the MainProcess from the previous signal handler call.
    safe_say('Waiting gracefully for cold shutdown to complete...', sys.__stdout__)


def on_cold_shutdown(worker: Worker):
    """Signal handler for cold shutdown.

    Registered for SIGQUIT and SIGINT (Ctrl+C) signals. If REMAP_SIGTERM is set to "SIGQUIT", this handler will also
    be registered for SIGTERM.

    This handler will initiate the cold (and soft if enabled) shutdown procesdure for the worker.

    Worker running with N tasks:
        - SIGTERM:
            -The worker will initiate the warm shutdown process until all tasks are finished. Additional.
            SIGTERM signals will be ignored. SIGQUIT will transition to the cold shutdown process described below.
        - SIGQUIT:
            - The worker will initiate the cold shutdown process.
            - If the soft shutdown is enabled, the worker will wait for the tasks to finish up to the soft
            shutdown timeout (practically having a limited warm shutdown just before the cold shutdown).
            - Cancel all tasks (from the MainProcess) and allow the worker to complete the cold shutdown
            process gracefully.

    Caveats:
        - SIGINT (Ctrl+C) signal is defined to replace itself with the cold shutdown (SIGQUIT) after first use,
        and to emit a message to the user to hit Ctrl+C again to initiate the cold shutdown process. But, most
        important, it will also be caught in WorkController.start() to initiate the warm shutdown process.
        - SIGTERM will also be handled in WorkController.start() to initiate the warm shutdown process (the same).
        - If REMAP_SIGTERM is set to "SIGQUIT", the SIGTERM signal will be remapped to SIGQUIT, and the cold
        shutdown process will be initiated instead of the warm shutdown process using SIGTERM.
        - If SIGQUIT is received (also via SIGINT) during the cold/soft shutdown process, the handler will cancel all
        unacked requests but still wait for the soft shutdown process to finish before terminating the worker
        gracefully. The next time the signal is received though, the worker will terminate immediately by force.

    So, the purpose of this handler is to allow waiting for the soft shutdown timeout, then cancel all tasks from
    the MainProcess and let the WorkController.terminate() to terminate the worker naturally. If the soft shutdown
    is disabled, it will immediately cancel all tasks let the cold shutdown finish normally.

    Args:
        worker (Worker): The worker that received the signal.
    """
    safe_say('worker: Hitting Ctrl+C again will terminate all running tasks!', sys.__stdout__)

    # Replace the signal handler for SIGINT (Ctrl+C) and SIGQUIT (and possibly SIGTERM)
    install_worker_term_hard_handler(worker, sig='SIGINT', callback=during_soft_shutdown)
    install_worker_term_hard_handler(worker, sig='SIGQUIT', callback=during_soft_shutdown)
    if REMAP_SIGTERM == "SIGQUIT":
        install_worker_term_hard_handler(worker, sig='SIGTERM', callback=during_soft_shutdown)
    # else, SIGTERM will print the _shutdown_handler's message and do nothing, every time it is received..

    # Initiate soft shutdown process (if enabled and tasks are running)
    worker.wait_for_soft_shutdown()

    # Cancel all unacked requests and allow the worker to terminate naturally
    worker.consumer.cancel_all_unacked_requests()

    # Stop the pool to allow successful tasks call on_success()
    worker.consumer.pool.stop()


# Allow SIGTERM to be remapped to SIGQUIT to initiate cold shutdown instead of warm shutdown using SIGTERM
if REMAP_SIGTERM == "SIGQUIT":
    install_worker_term_handler = partial(
        _shutdown_handler, sig='SIGTERM', how='Cold', callback=on_cold_shutdown, exitcode=EX_FAILURE,
    )
else:
    install_worker_term_handler = partial(
        _shutdown_handler, sig='SIGTERM', how='Warm',
    )


if not is_jython:  # pragma: no cover
    install_worker_term_hard_handler = partial(
        _shutdown_handler, sig='SIGQUIT', how='Cold', callback=on_cold_shutdown, exitcode=EX_FAILURE,
    )
else:  # pragma: no cover
    install_worker_term_handler = \
        install_worker_term_hard_handler = lambda *a, **kw: None


def on_SIGINT(worker):
    safe_say('worker: Hitting Ctrl+C again will initiate cold shutdown, terminating all running tasks!',
             sys.__stdout__)
    install_worker_term_hard_handler(worker, sig='SIGINT', verbose=False)


if not is_jython:  # pragma: no cover
    install_worker_int_handler = partial(
        _shutdown_handler, sig='SIGINT', callback=on_SIGINT,
        exitcode=EX_FAILURE,
    )
else:  # pragma: no cover
    def install_worker_int_handler(*args, **kwargs):
        pass


def _reload_current_worker():
    platforms.close_open_fds([
        sys.__stdin__, sys.__stdout__, sys.__stderr__,
    ])
    os.execv(sys.executable, [sys.executable] + sys.argv)


def install_worker_restart_handler(worker, sig='SIGHUP'):

    def restart_worker_sig_handler(*args):
        """Signal handler restarting the current python program."""
        set_in_sighandler(True)
        safe_say(f"Restarting celery worker ({' '.join(sys.argv)})",
                 sys.__stdout__)
        import atexit
        atexit.register(_reload_current_worker)
        from celery.worker import state
        state.should_stop = EX_OK
    platforms.signals[sig] = restart_worker_sig_handler


def install_cry_handler(sig='SIGUSR1'):
    # PyPy does not have sys._current_frames
    if is_pypy:  # pragma: no cover
        return

    def cry_handler(*args):
        """Signal handler logging the stack-trace of all active threads."""
        with in_sighandler():
            safe_say(cry())
    platforms.signals[sig] = cry_handler


def install_rdb_handler(envvar='CELERY_RDBSIG',
                        sig='SIGUSR2'):  # pragma: no cover

    def rdb_handler(*args):
        """Signal handler setting a rdb breakpoint at the current frame."""
        with in_sighandler():
            from celery.contrib.rdb import _frame, set_trace

            # gevent does not pass standard signal handler args
            frame = args[1] if args else _frame().f_back
            set_trace(frame)
    if os.environ.get(envvar):
        platforms.signals[sig] = rdb_handler


def install_HUP_not_supported_handler(worker, sig='SIGHUP'):

    def warn_on_HUP_handler(signum, frame):
        with in_sighandler():
            safe_say('{sig} not supported: Restarting with {sig} is '
                     'unstable on this platform!'.format(sig=sig))
    platforms.signals[sig] = warn_on_HUP_handler
