import socket
import sys
import traceback

from celery import __version__
from celery import beat
from celery import platforms
from celery.log import emergency_error
from celery.utils import get_full_cls_name, info, LOG_LEVELS
from celery.utils.info import humanize_seconds
from celery.utils import term

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . loader -> %(loader)s
    . scheduler -> %(scheduler)s
%(scheduler_info)s
    . logfile -> %(logfile)s@%(loglevel)s
    . maxinterval -> %(hmax_interval)s (%(max_interval)ss)
""".strip()


class Beat(object):
    Service = beat.Service

    def __init__(self, loglevel=None, logfile=None, schedule=None,
            max_interval=None, scheduler_cls=None, defaults=None,
            socket_timeout=30, redirect_stdouts=None,
            redirect_stdouts_level=None, **kwargs):
        """Starts the celerybeat task scheduler."""

        if defaults is None:
            from celery import conf as defaults
        self.defaults = defaults

        self.loglevel = loglevel or defaults.CELERYBEAT_LOG_LEVEL
        self.logfile = logfile or defaults.CELERYBEAT_LOG_FILE
        self.schedule = schedule or defaults.CELERYBEAT_SCHEDULE_FILENAME
        self.scheduler_cls = scheduler_cls or defaults.CELERYBEAT_SCHEDULER
        self.max_interval = max_interval
        self.socket_timeout = socket_timeout
        self.colored = term.colored(enabled=defaults.CELERYD_LOG_COLOR)
        self.redirect_stdouts = redirect_stdouts or defaults.REDIRECT_STDOUTS
        self.redirect_stdouts_level = (redirect_stdouts_level or
                                       defaults.REDIRECT_STDOUTS_LEVEL)

        if not isinstance(self.loglevel, int):
            self.loglevel = LOG_LEVELS[self.loglevel.upper()]

    def run(self):
        logger = self.setup_logging()
        print(str(self.colored.cyan(
                    "celerybeat v%s is starting." % __version__)))
        self.init_loader()
        self.set_process_title()
        self.start_scheduler(logger)

    def setup_logging(self):
        from celery import log
        handled = log.setup_logging_subsystem(loglevel=self.loglevel,
                                              logfile=self.logfile)
        if not handled:
            logger = log.get_default_logger(name="celery.beat")
            if self.redirect_stdouts:
                log.redirect_stdouts_to_logger(logger,
                        loglevel=self.redirect_stdouts_level)
        return logger

    def start_scheduler(self, logger=None):
        c = self.colored
        beat = self.Service(logger=logger,
                            max_interval=self.max_interval,
                            scheduler_cls=self.scheduler_cls,
                            schedule_filename=self.schedule)

        print(str(c.blue("__    ", c.magenta("-"),
                  c.blue("    ... __   "), c.magenta("-"),
                  c.blue("        _\n"),
                  c.reset(self.startup_info(beat)))))
        if self.socket_timeout:
            logger.debug("Setting default socket timeout to %r" % (
                self.socket_timeout))
            socket.setdefaulttimeout(self.socket_timeout)
        try:
            self.install_sync_handler(beat)
            beat.start()
        except Exception, exc:
            emergency_error(self.logfile,
                    "celerybeat raised exception %s: %s\n%s" % (
                            exc.__class__, exc, traceback.format_exc()))

    def init_loader(self):
        # Run the worker init handler.
        # (Usually imports task modules and such.)
        from celery.loaders import current_loader
        self.loader = current_loader()
        self.loader.init_worker()

    def startup_info(self, beat):
        return STARTUP_INFO_FMT % {
            "conninfo": info.format_broker_info(),
            "logfile": self.logfile or "[stderr]",
            "loglevel": LOG_LEVELS[self.loglevel],
            "loader": get_full_cls_name(self.loader.__class__),
            "scheduler": get_full_cls_name(beat.scheduler.__class__),
            "scheduler_info": beat.scheduler.info,
            "hmax_interval": humanize_seconds(beat.max_interval),
            "max_interval": beat.max_interval,
        }

    def set_process_title(self):
        arg_start = "manage" in sys.argv[0] and 2 or 1
        platforms.set_process_title("celerybeat",
                               info=" ".join(sys.argv[arg_start:]))

    def install_sync_handler(self, beat):
        """Install a ``SIGTERM`` + ``SIGINT`` handler that saves
        the celerybeat schedule."""

        def _sync(signum, frame):
            beat.sync()
            raise SystemExit()

        platforms.install_signal_handler("SIGTERM", _sync)
        platforms.install_signal_handler("SIGINT", _sync)


def run_celerybeat(*args, **kwargs):
    return Beat(*args, **kwargs).run()
