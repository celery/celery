from datetime import datetime
from itertools import count

from billiard.utils.functional import wraps

from django.db import models
from django.db import transaction
from django.db.models.query import QuerySet

from celery import states


def transaction_retry(max_retries=1):
    """Decorator for methods doing database operations.

    If the database operation fails, it will retry the operation
    at most ``max_retries`` times.

    """
    def _outer(fun):

        @wraps(fun)
        def _inner(*args, **kwargs):
            _max_retries = kwargs.pop("exception_retry_count", max_retries)
            for retries in count(0):
                try:
                    return fun(*args, **kwargs)
                except Exception: # pragma: no cover
                    # Depending on the database backend used we can experience
                    # various exceptions. E.g. psycopg2 raises an exception
                    # if some operation breaks the transaction, so saving
                    # the task result won't be possible until we rollback
                    # the transaction.
                    if retries >= _max_retries:
                        raise
                    transaction.rollback_unless_managed()

        return _inner

    return _outer


def update_model_with_dict(obj, fields):
    [setattr(obj, attr_name, attr_value)
        for attr_name, attr_value in fields.items()]
    obj.save()
    return obj


class ExtendedQuerySet(QuerySet):

    def update_or_create(self, **kwargs):
        obj, created = self.get_or_create(**kwargs)

        if not created:
            fields = dict(kwargs.pop("defaults", {}))
            fields.update(kwargs)
            update_model_with_dict(obj, fields)

        return obj


class ExtendedManager(models.Manager):

    def get_query_set(self):
        return ExtendedQuerySet(self.model)

    def update_or_create(self, **kwargs):
        return self.get_query_set().update_or_create(**kwargs)


class ResultManager(ExtendedManager):

    def get_all_expired(self):
        """Get all expired task results."""
        from celery import conf
        expires = conf.TASK_RESULT_EXPIRES
        return self.filter(date_done__lt=datetime.now() - expires)

    def delete_expired(self):
        """Delete all expired taskset results."""
        self.get_all_expired().delete()


class TaskManager(ResultManager):
    """Manager for :class:`celery.models.Task` models."""

    @transaction_retry(max_retries=1)
    def get_task(self, task_id):
        """Get task meta for task by ``task_id``.

        :keyword exception_retry_count: How many times to retry by
            transaction rollback on exception. This could theoretically
            happen in a race condition if another worker is trying to
            create the same task. The default is to retry once.

        """
        task, created = self.get_or_create(task_id=task_id)
        return task

    @transaction_retry(max_retries=2)
    def store_result(self, task_id, result, status, traceback=None):
        """Store the result and status of a task.

        :param task_id: task id

        :param result: The return value of the task, or an exception
            instance raised by the task.

        :param status: Task status. See
            :meth:`celery.result.AsyncResult.get_status` for a list of
            possible status values.

        :keyword traceback: The traceback at the point of exception (if the
            task failed).

        :keyword exception_retry_count: How many times to retry by
            transaction rollback on exception. This could theoretically
            happen in a race condition if another worker is trying to
            create the same task. The default is to retry twice.

        """
        return self.update_or_create(task_id=task_id, defaults={
                                        "status": status,
                                        "result": result,
                                        "traceback": traceback})


class TaskSetManager(ResultManager):
    """Manager for :class:`celery.models.TaskSet` models."""


    @transaction_retry(max_retries=1)
    def restore_taskset(self, taskset_id):
        """Get taskset meta for task by ``taskset_id``."""
        try:
            return self.get(taskset_id=taskset_id)
        except self.model.DoesNotExist:
            return None

    @transaction_retry(max_retries=2)
    def store_result(self, taskset_id, result):
        """Store the result of a taskset.

        :param taskset_id: task set id

        :param result: The return value of the taskset

        """
        return self.update_or_create(taskset_id=taskset_id,
                                     defaults={"result": result})
