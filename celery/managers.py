from django.db import models
from celery.registry import tasks
from datetime import datetime, timedelta


class PeriodicTaskManager(models.Manager):

    def get_waiting_tasks(self):
        periodic_tasks = tasks.get_all_periodic()
        waiting = []
        for task_name, task in periodic_tasks.items():
            task_meta, created = self.get_or_create(name=task_name)
            run_at = task_meta.last_run_at + timedelta(seconds=task.run_every)
            if datetime.now() > run_at:
                waiting.append(task_meta)
        return waiting
