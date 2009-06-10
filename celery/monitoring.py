from carrot.connection import DjangoAMQPConnection
from celery.messaging import StatsPublisher, StatsConsumer
from django.conf import settings


class Statistics(object):
    type = None

    def __init__(self, **kwargs):
        self.enabled = getattr(settings, "CELERY_STATISTICS", False)
        if not self.type:
            raise NotImplementedError(
                "Statistic classes must define their type.")

    def publish(self, **data):
        if not self.enabled:
            return
        connection = DjangoAMQPConnection()
        publisher = StatsPublisher(connection=connection)
        publisher.send({"type": self.type, "data": data})
        publisher.close()
        connection.close()

    @classmethod
    def start(cls, *args, **kwargs):
        stat = cls()
        stat.run()
        return stat

    def run(self, *args, **kwargs):
        if stat.enabled:
            stat.on_start(*args, **kwargs)

    def stop(self, *args, **kwargs):
        if self.enabled:
            self.on_finish(*args, **kwargs)


class TimerStats(Statistics):
    time_start = None

    def on_start(self, task_id, task_name, args, kwargs):
        self.task_id = task_id
        self.task_name = task_name
        self.args = self.args
        self.kwargs = self.kwargs
        self.time_start = time.time()
    
    def on_finish(self):
        nsecs = time.time() - self.time_start
        self.publish(task_id=task_id,
                     task_name=task_name,
                     args=args,
                     kwargs=kwargs,
                     nsecs=nsecs)


class TaskTimerStats(TimerStats):
    type = "task_time_running"


class StatsCollector(object):
    allowed_types = ["task_time_running"]
    total_tasks_processed = 0
    total_task_time_running = 0
    total_task_time_running_by_type = {}

    def collect(self):
        connection = DjangoAMQPConnection()
        consumer = StatsConsumer(connection=connection)
        it = consumer.iterqueue(infinite=False)
        for message in it:
            stats_entry = message.decode()
            stat_type = stats_entry["type"]
            if stat_type in self.allowed_types:
                handler = getattr(self, stat_type)
                handler(**stats_entry["data"])
        return self.on_cycle_end()

    def task_time_running(self, task_id, task_name, args, kwargs, nsecs):
        self.total_task_time_running += nsecs
        self.total_task_time_running_by_type[task_name] = \
                self.total_task_time_running_by_type.get(task_name, nsecs)
        self.total_task_time_running_by_type[task_name] += nsecs
        print("Task %s[%s](%s, %s): %d" % (
                task_id, task_name, args, kwargs, nsecs))

    def on_cycle_end(self):
        print("-" * 64)
        print("Total processing time by task type:")
        for task_name, nsecs in self.total_task_time_running_by_type.items():
            print("\t%s: %d" % (task_name, nsecs))
        print("Total task processing time: %d" % (
            self.total_task_time_running))
        print("Total tasks processed: %d" % self.total_tasks_processed)
