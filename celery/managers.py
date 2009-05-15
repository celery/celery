from django.db import models
from celery.registry import tasks
from datetime import datetime, timedelta


class TaskManager(models.Manager):
    
    def get_task(self, task_id):
        task, created = self.get_or_create(task_id=task_id)
        return task

    def is_done(self, task_id):
        return self.get_task(task_id).status == "DONE"

    def get_all_expired(self):
        return self.filter(date_done__lt=datetime.now() - timedelta(days=5),
                           status="DONE")

    def delete_expired(self):
        self.get_all_expired().delete()

    def store_result(self, task_id, result, status):
        task, created = self.get_or_create(task_id=task_id, defaults={
                                            "status": status,
                                            "result": result})
        if not created:
            task.status = status
            task.result = result
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

class RetryQueueManager(models.Manager):

    def add(self, task_name, task_id, args, kwargs):
        new_item, c = self.get_or_create(task_id=task_id, defaults={
                                            "task_name": task_name})
        new_item.stack = {"args": args, "kwargs": kwargs}
        new_item.save()
        return new_item

    def get_waiting_tasks(self):
        waiting_tasks = self.all().order_by('-last_retry', 'retry_count')
        to_retry = []
        for waiting_task in waiting_tasks:
            task = tasks[waiting_task.task_name]
            retry_at = waiting_task.last_retry_at + task.retry_interval
            if datetime.now() > retry_at:
                to_retry.append(waiting_task)
        return to_retry
