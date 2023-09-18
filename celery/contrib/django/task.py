import functools

try:
    from django.db import transaction
except ImportError:
    transaction = None

from celery.app.task import Task as BaseTask


class DjangoTask(BaseTask):
    """
    Extend the base :class:`~celery.app.task.Task` for Django.

    Provide a nicer API to trigger tasks at the end of the DB transaction.
    """

    def delay_on_commit(self, *args, **kwargs):
        """Call :meth:`~celery.app.task.Task.delay` with Django's ``on_commit()``."""
        return transaction.on_commit(functools.partial(self.delay, *args, **kwargs))

    def apply_async_on_commit(self, *args, **kwargs):
        """Call :meth:`~celery.app.task.Task.apply_async` with Django's ``on_commit()``."""
        return transaction.on_commit(functools.partial(self.apply_async, *args, **kwargs))
