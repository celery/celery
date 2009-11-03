import time
import shelve
import atexit
import threading
from UserDict import UserDict
from datetime import datetime
from celery import conf
from celery import registry
from celery.log import setup_logger



class SchedulingError(Exception):
    """An error occured while scheduling task."""


class ScheduleEntry(object):
    """An entry in the scheduler.

    :param task: The task class.
    :keyword last_run_at: The time and date when this task was last run.
    :keyword total_run_count: Total number of times this periodic task has
        been executed.

    """

    def __init__(self, task, last_run_at=None, total_run_count=None):
        self.task = task
        self.last_run_at = last_run_at or datetime.now()
        self.total_run_count = total_run_count or 0

    def execute(self):
        # Increment timestamps and counts before executing,
        # in case of exception.
        self.last_run_at = datetime.now()
        self.total_run_count += 1

        try:
            result = self.task.apply_async()
        except Exception, exc:
            raise SchedulingError(
                    "Couldn't apply scheduled task %s: %s" % (
                        self.task.name, exc))
        return result

    def is_due(self):
        return datetime.now() > (self.last_run_at + self.task.run_every)


class Scheduler(UserDict):
    """Scheduler for periodic tasks.

    :keyword registry: The task registry to use.
    :keyword schedule: The schedule dictionary. Default is the global
        persistent schedule ``celery.beat.schedule``.

    """
    interval = 1

    def __init__(self, **kwargs):

        def _get_default_logger():
            import multiprocessing
            return multiprocessing.get_logger()

        attr_defaults = {"registry": lambda: {},
                         "schedule": lambda: {},
                         "interval": lambda: self.interval,
                         "logger": _get_default_logger}

        for attr_name, attr_default_gen in attr_defaults.items():
            if attr_name in kwargs:
                attr_value = kwargs[attr_name]
            else:
                attr_value = attr_default_gen()
            setattr(self, attr_name, attr_value)

        self.schedule_registry()

    def tick(self):
        """Run a tick, that is one iteration of the scheduler.
        Executes all due tasks."""
        for entry in self.get_due_tasks():
            self.logger.debug("Scheduler: Sending due task %s" % (
                    entry.task.name))
            result = entry.execute()
            self.logger.debug("Scheduler: %s sent. id->%s" % (
                    entry.task_name, result.task_id))

    def get_due_tasks(self):
        """Get all the schedule entries that are due to execution."""
        return filter(lambda entry: entry.is_due(), self.schedule.values())

    def schedule_registry(self):
        """Add the current contents of the registry to the schedule."""
        periodic_tasks = self.registry.get_all_periodic()
        for name, task in self.registry.get_all_periodic().items():
            if name not in self.schedule:
                self.logger.debug(
                        "Scheduler: Adding periodic task %s to schedule" % (
                            task.name))
            self.schedule.setdefault(name, ScheduleEntry(task))

    @property
    def schedule(self):
        return self.data


class ClockService(object):
    scheduler_cls = Scheduler
    schedule_filename = conf.CELERYBEAT_SCHEDULE_FILENAME
    registry = registry.tasks

    def __init__(self, loglevel, logfile, is_detached=False):
        self.logger = setup_logger(loglevel, logfile)
        self._shutdown = threading.Event()
        self._stopped = threading.Event()

    def start(self):
        self.logger.info("ClockService: Starting...")
        schedule = shelve.open(filename=self.schedule_filename)
        #atexit.register(schedule.close)
        scheduler = self.scheduler_cls(schedule=schedule,
                                       registry=self.registry,
                                       logger=self.logger)
        self.logger.debug(
                "ClockService: Ticking with interval->%d, schedule->%s" % (
                    scheduler.interval, self.schedule_filename))

        synced = [False]
        def _stop():
            if not synced[0]:
                self.logger.debug("ClockService: Syncing schedule to disk...")
                schedule.sync()
                schedule.close()
                synced[0] = True
                self._stopped.set()

        try:
            while True:
                if self._shutdown.isSet():
                    break
                scheduler.tick()
                time.sleep(scheduler.interval)
        except (KeyboardInterrupt, SystemExit):
            _stop()
        finally:
            _stop()

    def stop(self, wait=False):
        self._shutdown.set()
        wait and self._stopped.wait() # block until shutdown done.


class ClockServiceThread(threading.Thread):

    def __init__(self, *args, **kwargs):
        self.clockservice = ClockService(*args, **kwargs)
        self.setDaemon(True)

    def run(self):
        self.clockservice.start()

    def stop(self):
        self.clockservice.stop(wait=True)
