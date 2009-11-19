"""celery.managers"""
from datetime import datetime
from django.db import models
from django.db import transaction

from celery.conf import TASK_RESULT_EXPIRES


class TaskManager(models.Manager):
    """Manager for :class:`celery.models.Task` models."""

    def get_task(self, task_id):
        """Get task meta for task by ``task_id``."""
        task, created = self.get_or_create(task_id=task_id)
        return task

    def is_successful(self, task_id):
        """Returns ``True`` if the task was executed successfully."""
        return self.get_task(task_id).status == "SUCCESS"

    def get_all_expired(self):
        """Get all expired task results."""
        return self.filter(date_done__lt=datetime.now() - TASK_RESULT_EXPIRES)

    def delete_expired(self):
        """Delete all expired task results."""
        self.get_all_expired().delete()

    def store_result(self, task_id, result, status, traceback=None,
            exception_retry=True):
        """Store the result and status of a task.

        :param task_id: task id

        :param result: The return value of the task, or an exception
            instance raised by the task.

        :param status: Task status. See
            :meth:`celery.result.AsyncResult.get_status` for a list of
            possible status values.

        :keyword traceback: The traceback at the point of exception (if the
            task failed).

        :keyword exception_retry: If True, we try a single retry with
            transaction rollback on exception
        """
        try:
            task, created = self.get_or_create(task_id=task_id, defaults={
                                                "status": status,
                                                "result": result,
                                                "traceback": traceback})
            if not created:
                task.status = status
                task.result = result
                task.traceback = traceback
                task.save()
        except Exception, exc:
            # depending on the database backend we can get various exceptions.
            # for excample, psycopg2 raises an exception if some operation
            # breaks transaction, and saving task result won't be possible
            # until we rollback transaction
            if exception_retry:
                transaction.rollback_unless_managed()
                self.store_result(task_id, result, status, traceback, False)
            else:
                raise
