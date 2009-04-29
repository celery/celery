from django.db import models
from celery.registry import tasks
from datetime import datetime, timedelta

__all__ = ["TaskManager", "PeriodicTaskManager"]


class TaskManager(models.Manager):
    
    def get_task(self, task_id):
        task, created = self.get_or_create(task_id=task_id)
        return task

    def is_done(self, task_id):
        return self.get_task(task_id).is_done

    def get_all_expired(self):
        return self.filter(date_done__lt=datetime.now() - timedelta(days=5),
                           is_done=True)

    def delete_expired(self):
        self.get_all_expired().delete()

    def mark_as_done(self, task_id):
        task, created = self.get_or_create(task_id=task_id, defaults={
                                            "is_done": True})
        if not created:
            task.is_done = True
            task.save()


class PeriodicTaskManager(models.Manager):

    def get_waiting_tasks(self):
        periodic_tasks = tasks.get_all_periodic()
        waiting = []
        for task_name, task in periodic_tasks.items():
            task_meta, created = self.get_or_create(name=task_name)
            # task_run.every must be a timedelta object.
            run_at = task_meta.last_run_at + task.run_every
            if datetime.now() > run_at:
                waiting.append(task_meta)
        return waiting
