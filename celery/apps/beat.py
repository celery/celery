import logging
import sys
import traceback

from celery import __version__
from celery import beat
from celery import platforms
from celery.log import emergency_error
from celery.utils import info, LOG_LEVELS

STARTUP_INFO_FMT = """
Configuration ->
    . broker -> %(conninfo)s
    . schedule -> %(schedule)s
    . logfile -> %(logfile)s@%(loglevel)s
""".strip()


class Beat(object):
    Service = beat.Service

    def __init__(self, loglevel=None, logfile=None, schedule=None,
            max_interval=None, scheduler_cls=None, defaults=None, **kwargs):
        """Starts the celerybeat task scheduler."""

        if defaults is None:
            from celery import conf as defaults
        self.defaults = defaults

        self.loglevel = loglevel or defaults.CELERYBEAT_LOG_LEVEL
        self.logfile = logfile or defaults.CELERYBEAT_LOG_FILE
        self.schedule = schedule or defaults.CELERYBEAT_SCHEDULE_FILENAME
        self.scheduler_cls = scheduler_cls
        self.max_interval = max_interval

        if not isinstance(self.loglevel, int):
            self.loglevel = LOG_LEVELS[self.loglevel.upper()]

    def run(self):
        logger = self.setup_logging()
        print("celerybeat v%s is starting." % __version__)
        self.init_loader()
        print(self.startup_info())
        self.set_process_title()
        print("celerybeat has started.")
        self.start_scheduler(logger)

    def setup_logging(self):
        from celery import log
        handled = log.setup_logging_subsystem(loglevel=self.loglevel,
                                              logfile=self.logfile)
        if not handled:
            logger = log.get_default_logger(name="celery.beat")
            log.redirect_stdouts_to_logger(logger, loglevel=logging.WARNING)
        return logger

    def start_scheduler(self, logger=None):
        beat = self.Service(logger=logger,
                            max_interval=self.max_interval,
                            scheduler_cls=self.scheduler_cls,
                            schedule_filename=self.schedule)

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
        current_loader().init_worker()

    def startup_info(self):
        return STARTUP_INFO_FMT % {
            "conninfo": info.format_broker_info(),
            "logfile": self.logfile or "@stderr",
            "loglevel": LOG_LEVELS[self.loglevel],
            "schedule": self.schedule,
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
