# -*- coding: utf-8 -*-
from __future__ import absolute_import

import atexit
import logging
try:
    import multiprocessing
except ImportError:
    multiprocessing = None  # noqa
import os
import socket
import sys
import warnings

from .. import __version__, platforms, signals
from ..app import app_or_default
from ..exceptions import ImproperlyConfigured, SystemTerminate
from ..utils import get_full_cls_name, isatty, LOG_LEVELS, cry
from ..worker import WorkController

try:
    from greenlet import GreenletExit
    IGNORE_ERRORS = (GreenletExit, )
except ImportError:
    IGNORE_ERRORS = ()


BANNER = """
 -------------- celery@%(hostname)s v%(version)s
---- **** -----
--- * ***  * -- [Configuration]
-- * - **** ---   . broker:      %(conninfo)s
- ** ----------   . loader:      %(loader)s
- ** ----------   . logfile:     %(logfile)s@%(loglevel)s
- ** ----------   . concurrency: %(concurrency)s
- ** ----------   . events:      %(events)s
- *** --- * ---   . beat:        %(celerybeat)s
-- ******* ----
--- ***** ----- [Queues]
 --------------   %(queues)s
"""

EXTRA_INFO_FMT = """
[Tasks]
%(tasks)s
"""


def cpu_count():
    if multiprocessing is not None:
        try:
            return multiprocessing.cpu_count()
        except NotImplementedError:
            pass
    return 2


def get_process_name():
    if multiprocessing is not None:
        return multiprocessing.current_process().name


class Worker(object):
    WorkController = WorkController

    def __init__(self, concurrency=None, loglevel=None, logfile=None,
            hostname=None, discard=False, run_clockservice=False,
            schedule=None, task_time_limit=None, task_soft_time_limit=None,
            max_tasks_per_child=None, queues=None, events=None, db=None,
            include=None, app=None, pidfile=None,
            redirect_stdouts=None, redirect_stdouts_level=None,
            autoscale=None, scheduler_cls=None, pool=None, **kwargs):
        self.app = app = app_or_default(app)
        conf = app.conf
        self.concurrency = (concurrency or
                            conf.CELERYD_CONCURRENCY or cpu_count())
        self.loglevel = loglevel or conf.CELERYD_LOG_LEVEL
        self.logfile = logfile or conf.CELERYD_LOG_FILE

        self.hostname = hostname or socket.gethostname()
        self.discard = discard
        self.run_clockservice = run_clockservice
        if self.app.IS_WINDOWS and self.run_clockservice:
            self.die("-B option does not work on Windows.  "
                     "Please run celerybeat as a separate service.")
        self.schedule = schedule or conf.CELERYBEAT_SCHEDULE_FILENAME
        self.scheduler_cls = scheduler_cls or conf.CELERYBEAT_SCHEDULER
        self.events = events if events is not None else conf.CELERY_SEND_EVENTS
        self.task_time_limit = (task_time_limit or
                                conf.CELERYD_TASK_TIME_LIMIT)
        self.task_soft_time_limit = (task_soft_time_limit or
                                     conf.CELERYD_TASK_SOFT_TIME_LIMIT)
        self.max_tasks_per_child = (max_tasks_per_child or
                                    conf.CELERYD_MAX_TASKS_PER_CHILD)
        self.redirect_stdouts = (redirect_stdouts or
                                 conf.CELERY_REDIRECT_STDOUTS)
        self.redirect_stdouts_level = (redirect_stdouts_level or
                                       conf.CELERY_REDIRECT_STDOUTS_LEVEL)
        self.pool = pool or conf.CELERYD_POOL
        self.db = db
        self.use_queues = [] if queues is None else queues
        self.queues = None
        self.include = [] if include is None else include
        self.pidfile = pidfile
        self.autoscale = None
        if autoscale:
            max_c, _, min_c = autoscale.partition(",")
            self.autoscale = [int(max_c), min_c and int(min_c) or 0]
        self._isatty = isatty(sys.stdout)

        self.colored = app.log.colored(self.logfile)

        if isinstance(self.use_queues, basestring):
            self.use_queues = self.use_queues.split(",")
        if isinstance(self.include, basestring):
            self.include = self.include.split(",")

        if not isinstance(self.loglevel, int):
            try:
                self.loglevel = LOG_LEVELS[self.loglevel.upper()]
            except KeyError:
                self.die("Unknown level %r. Please use one of %s." % (
                            self.loglevel,
                            "|".join(l for l in LOG_LEVELS.keys()
                                        if isinstance(l, basestring))))

    def run(self):
        self.init_loader()
        self.init_queues()
        self.worker_init()
        self.redirect_stdouts_to_logger()

        if getattr(os, "geteuid", None) and os.geteuid() == 0:
            warnings.warn(
                "Running celeryd with superuser privileges is not encouraged!")

        if self.discard:
            self.purge_messages()

        # Dump configuration to screen so we have some basic information
        # for when users sends bug reports.
        print(str(self.colored.cyan(" \n", self.startup_info())) +
              str(self.colored.reset(self.extra_info())))
        self.set_process_status("-active-")

        try:
            self.run_worker()
        except IGNORE_ERRORS:
            pass

    def on_consumer_ready(self, consumer):
        signals.worker_ready.send(sender=consumer)
        print("celery@%s has started." % self.hostname)

    def init_queues(self):
        if self.use_queues:
            create_missing = self.app.conf.CELERY_CREATE_MISSING_QUEUES
            try:
                self.app.amqp.queues.select_subset(self.use_queues,
                                                   create_missing)
            except KeyError, exc:
                raise ImproperlyConfigured(
                    "Trying to select queue subset of %r, but queue %s"
                    "is not defined in CELERY_QUEUES. If you want to "
                    "automatically declare unknown queues you have to "
                    "enable CELERY_CREATE_MISSING_QUEUES" % (
                        self.use_queues, exc))

    def init_loader(self):
        self.loader = self.app.loader
        self.settings = self.app.conf
        for module in self.include:
            self.loader.import_from_cwd(module)

    def redirect_stdouts_to_logger(self):
        handled = self.app.log.setup_logging_subsystem(loglevel=self.loglevel,
                                                       logfile=self.logfile)
        if not handled:
            logger = self.app.log.get_default_logger()
            if self.redirect_stdouts:
                self.app.log.redirect_stdouts_to_logger(logger,
                                loglevel=self.redirect_stdouts_level)

    def purge_messages(self):
        count = self.app.control.discard_all()
        what = (not count or count > 1) and "messages" or "message"
        print("discard: Erased %d %s from the queue.\n" % (count, what))

    def worker_init(self):
        # Run the worker init handler.
        # (Usually imports task modules and such.)
        self.loader.init_worker()

    def tasklist(self, include_builtins=True):
        from ..registry import tasks
        tasklist = tasks.keys()
        if not include_builtins:
            tasklist = filter(lambda s: not s.startswith("celery."),
                              tasklist)
        return "\n".join("  . %s" % task for task in sorted(tasklist))

    def extra_info(self):
        if self.loglevel <= logging.INFO:
            include_builtins = self.loglevel <= logging.DEBUG
            tasklist = self.tasklist(include_builtins=include_builtins)
            return EXTRA_INFO_FMT % {"tasks": tasklist}
        return ""

    def startup_info(self):
        app = self.app
        concurrency = self.concurrency
        if self.autoscale:
            cmax, cmin = self.autoscale
            concurrency = "{min=%s, max=%s}" % (cmin, cmax)
        return BANNER % {
            "hostname": self.hostname,
            "version": __version__,
            "conninfo": self.app.broker_connection().as_uri(),
            "concurrency": concurrency,
            "loglevel": LOG_LEVELS[self.loglevel],
            "logfile": self.logfile or "[stderr]",
            "celerybeat": "ON" if self.run_clockservice else "OFF",
            "events": "ON" if self.events else "OFF",
            "loader": get_full_cls_name(self.loader.__class__),
            "queues": app.amqp.queues.format(indent=18, indent_first=False),
        }

    def run_worker(self):
        if self.pidfile:
            pidlock = platforms.create_pidlock(self.pidfile).acquire()
            atexit.register(pidlock.release)
        worker = self.WorkController(app=self.app,
                                concurrency=self.concurrency,
                                loglevel=self.loglevel,
                                logfile=self.logfile,
                                hostname=self.hostname,
                                ready_callback=self.on_consumer_ready,
                                embed_clockservice=self.run_clockservice,
                                schedule_filename=self.schedule,
                                scheduler_cls=self.scheduler_cls,
                                send_events=self.events,
                                db=self.db,
                                max_tasks_per_child=self.max_tasks_per_child,
                                task_time_limit=self.task_time_limit,
                                task_soft_time_limit=self.task_soft_time_limit,
                                autoscale=self.autoscale,
                                pool_cls=self.pool)
        self.install_platform_tweaks(worker)
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
                # See http://github.com/ask/celery/issues#issue/152
                install_HUP_not_supported_handler(worker)
            else:
                install_worker_restart_handler(worker)
        install_worker_term_handler(worker)
        install_worker_int_handler(worker)
        install_cry_handler(worker.logger)
        install_rdb_handler()
        signals.worker_init.send(sender=worker)

    def osx_proxy_detection_workaround(self):
        """See http://github.com/ask/celery/issues#issue/161"""
        os.environ.setdefault("celery_dummy_proxy", "set_by_celeryd")

    def set_process_status(self, info):
        info = "%s (%s)" % (info, platforms.strargv(sys.argv))
        return platforms.set_mp_process_title("celeryd",
                                              info=info,
                                              hostname=self.hostname)

    def die(self, msg, exitcode=1):
        sys.stderr.write("Error: %s\n" % (msg, ))
        sys.exit(exitcode)


def install_worker_int_handler(worker):

    def _stop(signum, frame):
        process_name = get_process_name()
        if not process_name or process_name == "MainProcess":
            print("celeryd: Hitting Ctrl+C again will terminate "
                  "all running tasks!")
            install_worker_int_again_handler(worker)
            print("celeryd: Warm shutdown (%s)" % (process_name, ))
            worker.stop(in_sighandler=True)
        raise SystemExit()

    platforms.signals["SIGINT"] = _stop


def install_worker_int_again_handler(worker):

    def _stop(signum, frame):
        process_name = get_process_name()
        if not process_name or process_name == "MainProcess":
            print("celeryd: Cold shutdown (%s)" % (process_name, ))
            worker.terminate(in_sighandler=True)
        raise SystemTerminate()

    platforms.signals["SIGINT"] = _stop


def install_worker_term_handler(worker):

    def _stop(signum, frame):
        process_name = get_process_name()
        if not process_name or process_name == "MainProcess":
            print("celeryd: Warm shutdown (%s)" % (process_name, ))
            worker.stop(in_sighandler=True)
        raise SystemExit()

    platforms.signals["SIGTERM"] = _stop


def install_worker_restart_handler(worker):

    def restart_worker_sig_handler(signum, frame):
        """Signal handler restarting the current python program."""
        print("Restarting celeryd (%s)" % (" ".join(sys.argv), ))
        worker.stop(in_sighandler=True)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    platforms.signals["SIGHUP"] = restart_worker_sig_handler


def install_cry_handler(logger):
    # Jython/PyPy does not have sys._current_frames
    is_jython = sys.platform.startswith("java")
    is_pypy = hasattr(sys, "pypy_version_info")
    if not (is_jython or is_pypy):

        def cry_handler(signum, frame):
            """Signal handler logging the stacktrace of all active threads."""
            logger.error("\n" + cry())

        platforms.signals["SIGUSR1"] = cry_handler


def install_rdb_handler(envvar="CELERY_RDBSIG"):  # pragma: no cover

    def rdb_handler(signum, frame):
        """Signal handler setting a rdb breakpoint at the current frame."""
        from ..contrib import rdb
        rdb.set_trace(frame)

    if os.environ.get(envvar):
        platforms.signals["SIGUSR2"] = rdb_handler


def install_HUP_not_supported_handler(worker):

    def warn_on_HUP_handler(signum, frame):
        worker.logger.error("SIGHUP not supported: "
            "Restarting with HUP is unstable on this platform!")

    platforms.signals["SIGHUP"] = warn_on_HUP_handler
