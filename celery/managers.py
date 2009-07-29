"""celery.managers"""
from django.db import models
from django.db import connection
from celery.registry import tasks
from celery.conf import TASK_RESULT_EXPIRES
from datetime import datetime, timedelta
from django.conf import settings
import random

# server_drift can be negative, but timedelta supports addition on
# negative seconds.
SERVER_DRIFT = timedelta(seconds=random.vonmisesvariate(1, 4))


class TaskManager(models.Manager):
    """Manager for :class:`celery.models.Task` models."""

    def get_task(self, task_id):
        """Get task meta for task by ``task_id``."""
        task, created = self.get_or_create(task_id=task_id)
        return task

    def is_done(self, task_id):
        """Returns ``True`` if the task was executed successfully."""
        return self.get_task(task_id).status == "DONE"

    def get_all_expired(self):
        """Get all expired task results."""
        return self.filter(date_done__lt=datetime.now() - TASK_RESULT_EXPIRES)

    def delete_expired(self):
        """Delete all expired task results."""
        self.get_all_expired().delete()

    def store_result(self, task_id, result, status):
        """Store the result and status of a task.

        :param task_id: task id

        :param result: The return value of the task, or an exception
            instance raised by the task.

        :param status: Task status. See
            :meth:`celery.result.AsyncResult.get_status` for a list of
            possible status values.

        """
        task, created = self.get_or_create(task_id=task_id, defaults={
                                            "status": status,
                                            "result": result})
        if not created:
            task.status = status
            task.result = result
            task.save()


class PeriodicTaskManager(models.Manager):
    """Manager for :class:`celery.models.PeriodicTask` models."""

    def lock(self):
        """Lock the periodic task table for reading."""
        if settings.DATABASE_ENGINE != "mysql":
            return
        cursor = connection.cursor()
        table = self.model._meta.db_table
        cursor.execute("LOCK TABLES %s READ" % table)
        row = cursor.fetchone()
        return row

    def unlock(self):
        """Unlock the periodic task table."""
        if settings.DATABASE_ENGINE != "mysql":
            return
        cursor = connection.cursor()
        table = self.model._meta.db_table
        cursor.execute("UNLOCK TABLES")
        row = cursor.fetchone()
        return row

    def init_entries(self):
        """Add entries for all registered periodic tasks.

        Should be run at worker start.
        """
        periodic_tasks = tasks.get_all_periodic()
        for task_name in periodic_tasks.keys():
            task_meta, created = self.get_or_create(name=task_name)

    def is_time(self, last_run_at, run_every):
        """Check if if it is time to run the periodic task.

        :param last_run_at: Last time the periodic task was run.
        :param run_every: How often to run the periodic task.

        :rtype bool:

        """
        run_every_drifted = run_every + SERVER_DRIFT
        run_at = last_run_at + run_every_drifted
        if datetime.now() > run_at:
            return True
        return False

    def get_waiting_tasks(self):
        """Get all waiting periodic tasks.

        :returns: list of :class:`celery.models.PeriodicTaskMeta` objects.
        """
        periodic_tasks = tasks.get_all_periodic()

        # Find all periodic tasks to be run.
        waiting = []
        for task_meta in self.all():
            if task_meta.name in periodic_tasks:
                task = periodic_tasks[task_meta.name]
                run_every = task.run_every
                if self.is_time(task_meta.last_run_at, run_every):
                    # Get the object again to be sure noone else
                    # has already taken care of it.
                    self.lock()
                    try:
                        secure = self.get(pk=task_meta.pk)
                        if self.is_time(secure.last_run_at, run_every):
                            secure.last_run_at = datetime.now()
                            secure.save()
                            waiting.append(secure)
                    finally:
                        self.unlock()
        return waiting
