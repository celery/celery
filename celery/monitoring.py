from carrot.connection import DjangoAMQPConnection
from celery.messaging import StatsPublisher, StatsConsumer
from django.conf import settings


class Statistics(object):
    type = None

    def __init__(self, **kwargs):
        self.enabled = getattr(settings, "CELERY_STATISTICS", False))
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
    allowed_stats = ["task_time_running"]

    def run(self):
        connection = DjangoAMQPConnection()
        consumer = StatsConsumer(connection=connection)
        it = consumer.iterqueue(infinite=False)
        total = 0
        for message in it:
            data = message.decode()
            stat_name = data.get("type")
            if stat_name in self.allowed_stats:
                handler = getattr(self, stat_name)
                handler(**data["data"])
                total += 1
        return total

    def task_time_running(self, task_id, task_name, args, kwargs, nsecs):
        print("Task %s[%s](%s, %s): %d" % (
                task_id, task_name, args, kwargs, nsecs))
