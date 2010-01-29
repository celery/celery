from celery.task import tasks, Task


class SomeAppTask(Task):
    name = "c.unittest.SomeAppTask"

    def run(self, **kwargs):
        return 42
