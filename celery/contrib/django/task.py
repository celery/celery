import functools

from django.db import transaction

from celery.app.task import Task


class DjangoTask(Task):
    """
    Extend the base :class:`~celery.app.task.Task` for Django.

    Provide a nicer API to trigger tasks at the end of the DB transaction.
    """

    def delay_on_commit(self, *args, **kwargs) -> None:
        """Call :meth:`~celery.app.task.Task.delay` with Django's ``on_commit()``."""
        transaction.on_commit(functools.partial(self.delay, *args, **kwargs))

    def apply_async_on_commit(self, *args, **kwargs) -> None:
        """Call :meth:`~celery.app.task.Task.apply_async` with Django's ``on_commit()``."""
        transaction.on_commit(functools.partial(self.apply_async, *args, **kwargs))
