from celery.task import Task


class MyTask(Task):

    def run(self, x, y):
        return x * y
