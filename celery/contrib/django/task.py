import functools

from django.db import transaction

from celery.app.task import Task as BaseTask


class Task(BaseTask):
    """
    Extend the base task to work nicely with Django.

    Provide a better API to trigger tasks at the end of the DB transaction.
    """

    def delay_on_commit(self, *args, **kwargs):
        """Call delay() with Django's on_commit()."""
        return transaction.on_commit(functools.partial(self.delay, *args, **kwargs))

    def apply_async_on_commit(self, *args, **kwargs):
        """Call apply_async() with Django's on_commit()."""
        return transaction.on_commit(functools.partial(self.apply_async, *args, **kwargs))
