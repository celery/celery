from celery.task import Task
from celery.registry import tasks


class MyTask(Task):

    def run(self, x, y):
        return x * y
tasks.register(MyTask)
