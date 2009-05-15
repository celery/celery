from carrot.connection import DjangoAMQPConnection
from celery.messaging import TaskConsumer
from celery.conf import DAEMON_CONCURRENCY, DAEMON_LOG_FILE
from celery.conf import QUEUE_WAKEUP_AFTER, EMPTY_MSG_EMIT_EVERY
from celery.log import setup_logger
from celery.registry import tasks
from celery.process import ProcessQueue
from celery.models import PeriodicTaskMeta
from celery.task import mark_as_done, mark_as_failure
import multiprocessing
import simplejson
import traceback
import logging
import time


class EmptyQueue(Exception):
    """The message queue is currently empty."""


class UnknownTask(Exception):
    """Got an unknown task in the queue. The message is requeued and
    ignored."""


def jail(task_id, callable_, args, kwargs):
    try:
        result = callable_(*args, **kwargs)
        mark_as_done(task_id, result)
        print("SUCCESS: %s" % result)
        return result
    except Exception, exc:
        mark_as_failure(task_id, exc)
        print("FAILURE: %s\n%s" % (exc, traceback.format_exc()))
        return exc


class TaskWrapper(object):
    def __init__(self, task_name, task_id, task_func, args, kwargs):
        self.task_name = task_name
        self.task_id = task_id
        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def from_message(cls, message):
        message_data = simplejson.loads(message.body)
        task_name = message_data["task"]
        task_id = message_data["id"]
        args = message_data["args"]
        kwargs = message_data["kwargs"]
        if task_name not in tasks:
            message.reject()
            raise UnknownTask(task_name)
        task_func = tasks[task_name]
        return cls(task_name, task_id, task_func, args, kwargs)

    def extend_kwargs_with_logging(self, loglevel, logfile):
        task_func_kwargs = {"logfile": logfile,
                            "loglevel": loglevel,
                            "task_id": self.task_id,
                            "task_name": self.task_name}
        task_func_kwargs.update(self.kwargs)
        return task_func_kwargs

    def execute(self, loglevel, logfile):
        task_func_kwargs = self.extend_kwargs_with_logging(loglevel, logfile)
        return jail(self.task_id, [
                        self.task_func, self.args, task_func_kwargs])

    def execute_using_pool(self, pool, loglevel, logfile):
        task_func_kwargs = self.extend_kwargs_with_logging(loglevel, logfile)
        return pool.apply_async(jail, [self.task_id, self.task_func,
                                       self.args, task_func_kwargs])


class EventTimer(object):
    """Do something at an interval."""

    def __init__(self, event, interval=None):
        self.event = event
        self.interval = interval
        self.last_triggered = None

    def tick(self):
        if not self.interval: # never trigger if no interval.
            return
        if not self.last_triggered or \
                time.time() > self.last_triggered + self.interval:
            self.event()
            self.last_triggered = time.time()


class TaskDaemon(object):
    """Executes tasks waiting in the task queue.

    ``concurrency`` is the number of simultaneous processes.
    """
    loglevel = logging.ERROR
    concurrency = DAEMON_CONCURRENCY
    logfile = DAEMON_LOG_FILE
    queue_wakeup_after = QUEUE_WAKEUP_AFTER
    empty_msg_emit_every = EMPTY_MSG_EMIT_EVERY
    
    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            queue_wakeup_after=None):
        self.loglevel = loglevel or self.loglevel
        self.concurrency = concurrency or self.concurrency
        self.logfile = logfile or self.logfile
        self.queue_wakeup_after = queue_wakeup_after or \
                                    self.queue_wakeup_after
        self.logger = setup_logger(loglevel, logfile)
        self.pool = multiprocessing.Pool(self.concurrency)
        self.reset_connection()

    def reset_connection(self):
        if hasattr(self, "task_consumer"):
            self.task_consumer.close()
        self.task_consumer = TaskConsumer(connection=DjangoAMQPConnection())

    def connection_diagnostics(self):
        if not self.task_consumer.channel.connection:
            self.reset_connection()

    def fetch_next_task(self):
        message = self.task_consumer.fetch()
        if message is None: # No messages waiting.
            raise EmptyQueue()

        task = TaskWrapper.from_message(message)
        self.logger.info("Got task from broker: %s[%s]" % (
                            task.task_name, task.task_id))

        return task, message

    def execute_next_task(self):
        task, message = self.fetch_next_task()

        try:
            result = task.execute_using_pool(self.pool, self.loglevel,
                                             self.logfile)
        except Exception, error:
            self.logger.critical("Worker got exception %s: %s\n%s" % (
                error.__class__, error, traceback.format_exc()))
            return 

        message.ack()
        return result, task.task_name, task.task_id

    def run_periodic_tasks(self):
        """Schedule all waiting periodic tasks for execution.
       
        Returns list of :class:`celery.models.PeriodicTaskMeta` objects.
        """
        waiting_tasks = PeriodicTaskMeta.objects.get_waiting_tasks()
        [waiting_task.delay()
                for waiting_task in waiting_tasks]
        return waiting_tasks

    def schedule_retry_tasks(self):
        """Reschedule all requeued tasks waiting for retry."""
        pass

    def run(self):
        """The worker server's main loop."""
        results = ProcessQueue(self.concurrency, logger=self.logger,
                done_msg="Task %(name)s[%(id)s] processed: %(return_value)s")
        log_wait = lambda: self.logger.info("Waiting for queue...")
        ev_msg_waiting = EventTimer(log_wait, self.empty_msg_emit_every)
        events = [
            EventTimer(self.run_periodic_tasks, 1),
            EventTimer(self.schedule_retry_tasks, 2),
            EventTimer(self.connection_diagnostics, 3),
            EventTimer(self.reset_connection, 60 * 5),
        ]

        while True:
            [event.tick() for event in events]
            try:
                result, task_name, task_id = self.execute_next_task()
            except ValueError:
                # execute_next_task didn't return a r/name/id tuple,
                # probably because it got an exception.
                continue
            except EmptyQueue:
                ev_msg_waiting.tick()
                time.sleep(self.queue_wakeup_after)
                continue
            except UnknownTask, e:
                self.logger.info("Unknown task ignored: %s" % (e))
                continue
            except Exception, e:
                self.logger.critical("Message queue raised %s: %s\n%s" % (
                             e.__class__, e, traceback.format_exc()))
                continue
           
            results.add(result, task_name, task_id)
