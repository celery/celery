"""

    Publishing Statistics and Monitoring Celery.

"""
from carrot.connection import DjangoAMQPConnection
from celery.messaging import StatsPublisher, StatsConsumer
from django.conf import settings
from django.core.cache import cache
import time

DEFAULT_CACHE_KEY_PREFIX = "celery-statistics"


class Statistics(object):
    """Base class for classes publishing celery statistics.

    .. attribute:: type

        **REQUIRED** The type of statistics this class handles.

    **Required handlers**

        * on_start()

        * on_stop()

    """
    type = None

    def __init__(self, **kwargs):
        self.enabled = getattr(settings, "CELERY_STATISTICS", False)
        if not self.type:
            raise NotImplementedError(
                "Statistic classes must define their type.")

    def publish(self, **data):
        """Publish statistics to be collected later by
        :class:`StatsCollector`.

        :param data: An arbitrary Python object containing the statistics
            to be published.

        """
        if not self.enabled:
            return
        connection = DjangoAMQPConnection()
        publisher = StatsPublisher(connection=connection)
        publisher.send({"type": self.type, "data": data})
        publisher.close()
        connection.close()

    @classmethod
    def start(cls, *args, **kwargs):
        """Convenience method instantiating and running :meth:`run` in
        one swoop."""
        stat = cls()
        stat.run(*args, **kwargs)
        return stat

    def run(self, *args, **kwargs):
        """Start producing statistics."""
        if self.enabled:
            return self.on_start(*args, **kwargs)

    def stop(self, *args, **kwargs):
        """Stop producing and publish statistics."""
        if self.enabled:
            return self.on_finish(*args, **kwargs)

    def on_start(self, *args, **kwargs):
        """What to do when the :meth:`run` method is called."""
        raise NotImplementedError(
                "Statistics classes must define a on_start handler.")

    def on_stop(self, *args, **kwargs):
        """What to do when the :meth:`stop` method is called."""
        raise NotImplementedError(
                "Statistics classes must define a on_stop handler.")


class TimerStats(Statistics):
    """A generic timer producing ``celery`` statistics.

    .. attribute:: time_start

        The time when this class was instantiated (in :func:`time.time`
        format.)

    """
    time_start = None

    def on_start(self, task_id, task_name, args, kwargs):
        """What to do when the timers :meth:`run` method is called."""
        self.task_id = task_id
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs
        self.time_start = time.time()

    def on_finish(self):
        """What to do when the timers :meth:`stop` method is called.

        :returns: the time in seconds it took between calling :meth:`start` on
            this class and :meth:`stop`.
        """
        nsecs = time.time() - self.time_start
        self.publish(task_id=self.task_id,
                     task_name=self.task_name,
                     args=self.args,
                     kwargs=self.kwargs,
                     nsecs=str(nsecs))
        return nsecs


class TaskTimerStats(TimerStats):
    """Time a running :class:`celery.task.Task`."""
    type = "task_time_running"


class StatsCollector(object):
    """Collect and report Celery statistics.

    **NOTE**: Please run only one collector at any time, or your stats
        will be skewed.

    .. attribute:: total_tasks_processed

        The number of tasks executed in total since the first time
        :meth:`collect` was executed on this class instance.

    .. attribute:: total_tasks_processed_by_type

        A dictionary of task names and how many times they have been
        executed in total since the first time :meth:`collect` was executed
        on this class instance.

    .. attribute:: total_task_time_running

        The total time, in seconds, it took to process all the tasks executed
        since the first time :meth:`collect` was executed on this class
        instance.

    .. attribute:: total_task_time_running_by_type

        A dictionary of task names and their total running time in seconds,
        counting all the tasks that has been run since the first time
        :meth:`collect` was executed on this class instance.

    **NOTE**: You have to run :meth:`collect` for these attributes
        to be filled.


    """

    allowed_types = ["task_time_running"]

    def __init__(self):
        self.total_tasks_processed = 0
        self.total_tasks_processed_by_type = {}
        self.total_task_time_running = 0.0
        self.total_task_time_running_by_type = {}

    def collect(self):
        """Collect any new statistics available since the last time
        :meth:`collect` was executed."""
        connection = DjangoAMQPConnection()
        consumer = StatsConsumer(connection=connection)
        it = consumer.iterqueue(infinite=False)
        for message in it:
            stats_entry = message.decode()
            stat_type = stats_entry["type"]
            if stat_type in self.allowed_types:
                # Decode keys to unicode for use as kwargs.
                data = dict((key.encode("utf-8"), value)
                                for key, value in stats_entry["data"].items())
                handler = getattr(self, stat_type)
                handler(**data)

    def dump_to_cache(self, cache_key_prefix=DEFAULT_CACHE_KEY_PREFIX):
        """Store collected statistics in the cache."""
        cache.set("%s-total_tasks_processed" % cache_key_prefix,
                self.total_tasks_processed)
        cache.set("%s-total_tasks_processed_by_type" % cache_key_prefix,
                    self.total_tasks_processed_by_type)
        cache.set("%s-total_task_time_running" % cache_key_prefix,
                    self.total_task_time_running)
        cache.set("%s-total_task_time_running_by_type" % cache_key_prefix,
                    self.total_task_time_running_by_type)

    def task_time_running(self, task_id, task_name, args, kwargs, nsecs):
        """Process statistics regarding how long a task has been running
        (the :class:TaskTimerStats` class is responsible for sending these).

        :param task_id: The UUID of the task.
        :param task_name: The name of task.
        :param args: The tasks positional arguments.
        :param kwargs: The tasks keyword arguments.
        :param nsecs: The number of seconds (in :func:`time.time` format)
            it took to execute the task.

        """
        nsecs = float(nsecs)
        self.total_tasks_processed += 1
        self.total_task_time_running += nsecs
        if task_name not in self.total_task_time_running_by_type:
            self.total_task_time_running_by_type[task_name] = nsecs
        else:
            self.total_task_time_running_by_type[task_name] += nsecs
        if task_name not in self.total_tasks_processed_by_type:
            self.total_tasks_processed_by_type[task_name] = 1
        else:
            self.total_tasks_processed_by_type[task_name] += 1

    def report(self):
        """Dump a nice statistics report from the data collected since
        the first time :meth:`collect` was executed on this instance.

        It outputs the following information:

            * Total processing time by task type and how many times each
                task has been excuted.

            * Total task processing time.

            * Total number of tasks executed

        """
        print("Total processing time by task type:")
        for task_name, nsecs in self.total_task_time_running_by_type.items():
            print("\t%s: %s secs. (for a total of %d executed.)" % (
                    task_name, nsecs,
                    self.total_tasks_processed_by_type.get(task_name)))
        print("Total task processing time: %s secs." % (
            self.total_task_time_running))
        print("Total tasks processed: %d" % self.total_tasks_processed)
