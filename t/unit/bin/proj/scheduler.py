from celery.beat import Scheduler


class mScheduler(Scheduler):
    def tick(self):
        raise Exception
