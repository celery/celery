from carrot.connection import DjangoAMQPConnection
from crunchy.messaging import TaskConsumer
from crunchy.conf import DAEMON_CONCURRENCY, DAEMON_LOG_FILE
from crunchy.conf import QUEUE_WAKEUP_AFTER, EMPTY_MSG_EMIT_EVERY
from crunchy.log import setup_logger
from crunchy.registry import tasks
from crunchy.process import ProcessQueue
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


class TaskDaemon(object):
    """Refreshes feed_urls in the queue using a process pool.

    ``concurrency`` is the number of simultaneous processes.
    """
    loglevel = logging.ERROR
    concurrency = DAEMON_CONCURRENCY
    logfile = DAEMON_LOG_FILE
    queue_wakeup_after = QUEUE_WAKEUP_AFTER
    
    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            queue_wakeup_after=None):
        self.loglevel = loglevel or self.loglevel
        self.concurrency = concurrency or self.concurrency
        self.logfile = logfile or self.logfile
        self.queue_wakeup_after = queue_wakeup_after or \
                                    self.queue_wakeup_after
        self.logger = setup_logger(loglevel, logfile)
        self.pool = multiprocessing.Pool(self.concurrency)
        self.task_consumer = TaskConsumer(connection=DjangoAMQPConnection)
        self.task_registry = tasks

    def fetch_next_task(self):
        message = self.task_consumer.fetch()
        if message is None: # No messages waiting.
            raise EmptyQueue()

        message_data = simplejson.loads(message.body)
        task_name = message_data.pop("crunchTASK")
        task_id = message_data.pop("crunchID")
        self.logger.info("Got task from broker: %s[%s]" % (
                            task_name, task_id))
        if task_name not in self.task_registry:
            message.reject()
            raise UnknownTask(task_name)

        task_func = self.task_registry[task_name]
        task_func_params = {"loglevel": self.loglevel,
                            "logfile": self.logfile}
        task_func_params.update(message_data)

        #try:
        result = self.pool.apply_async(task_func, [], task_func_params)
        #except:
        #    message.reject()
        #    raise


        message.ack()
        return result, task_name, task_id

    def run(self):
        results = ProcessQueue(self.concurrency, logger=self.logger,
                done_msg="Task %(name)s[%(id)s] processed: %(return_value)s")
        last_empty_emit = None

        while True:
            try:
                result, task_name, task_id = self.fetch_next_task()
            except EmptyQueue:
                if not last_empty_emit or \
                        time.time() > last_empty_emit + EMPTY_MSG_EMIT_EVERY:
                    self.logger.info("Waiting for queue.")
                    last_empty_emit = time.time()
                time.sleep(self.queue_wakeup_after)
                continue
            except UnknownTask, e:
                self.logger.info("Unknown task %s requeued and ignored." % (
                                    task_name))
                continue
            #except Exception, e:
            #    self.logger.critical("Raised %s: %s\n%s" % (
            #                 e.__class__, e, traceback.format_exc()))
            #    continue
           
            results.add(result, task_name, task_id)
