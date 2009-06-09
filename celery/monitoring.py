from carrot.connection import DjangoAMQPConnection
from celery.messaging import StatsPublisher, StatsConsumer
from django.conf import settings

def send_statistics(stat_name, **data):
    send_stats = getattr(settings, "CELERY_STATISTICS", False)
    if send_stats:
        connection = DjangoAMQPConnection()
        publisher = StatsPublisher(connection=connection)
        publisher.send({"stat_name": stat_name, "data": data})
        publisher.close()
        connection.close()


class Statistics(object):

    def task_time_running(self, task_id, task_name, args, kwargs, nsecs):
        send_statistics("task_time_running",
                        task_id=task_id,
                        task_name=task_name,
                        args=args,
                        kwargs=kwargs,
                        nsecs=nsecs)


class StatsCollector(object):
    allowed_stats = ["task_time_running"]

    def run(self):
        connection = DjangoAMQPConnection()
        consumer = StatsConsumer(connection=connection)
        it = consumer.iterqueue(infinite=False)
        total = 0
        for message in it:
            data = message.decode()
            stat_name = data.get("stat_name")
            if stat_name in self.allowed_stats:
                handler = getattr(self, stat_name)
                handler(**data["data"])
                total += 1
        return total

    def task_time_running(self, task_id, task_name, args, kwargs, nsecs):
        print("Task %s[%s](%s, %s): %d" % (
                task_id, task_name, args, kwargs, nsecs))
