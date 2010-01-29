================
 Periodic Tasks
================

You can schedule tasks to run at intervals like ``cron``.
Here's an example of a periodic task:

.. code-block:: python

    from celery.task import PeriodicTask
    from celery.registry import tasks
    from datetime import timedelta

    class MyPeriodicTask(PeriodicTask):
        run_every = timedelta(seconds=30)

        def run(self, **kwargs):
            logger = self.get_logger(**kwargs)
            logger.info("Running periodic task!")
    >>> tasks.register(MyPeriodicTask)


If you want to use periodic tasks you need to start the ``celerybeat``
service. You have to make sure only one instance of this server is running at
any time, or else you will end up with multiple executions of the same task.

To start the ``celerybeat`` service::

    $ celerybeat

or if using Django::

    $ python manage.py celerybeat


You can also start ``celerybeat`` with ``celeryd`` by using the ``-B`` option,
this is convenient if you only have one server::

    $ celeryd -B

or if using Django::

    $ python manage.py celeryd  -B
