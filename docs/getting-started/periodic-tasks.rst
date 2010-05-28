================
 Periodic Tasks
================

You can schedule tasks to run at intervals like ``cron``.
Here's an example of a periodic task:

.. code-block:: python

    from celery.decorators import periodic_task
    from datetime import timedelta

    @periodic_task(run_every=timedelta(seconds=30))
    def every_30_seconds(\*\*kwargs):
        logger = self.get_logger(\*\*kwargs)
        logger.info("Running periodic task!")

If you want a little more control over when the task is executed, for example,
a particular time of day or day of the week, you can use the ``crontab`` schedule
type:

.. code-block:: python

    from celery.task.schedules import crontab
    from celery.decorators import periodic_task

    @periodoc_task(run_every=crontab(hour=7, minute=30, day_of_week=1))
    def every_monday_morning(\*\*kwargs):
        logger = self.get_logger(\*\*kwargs)
        logger.info("Execute every Monday at 7:30AM.")

If you want to use periodic tasks you need to start the ``celerybeat``
service. You have to make sure only one instance of this server is running at
any time, or else you will end up with multiple executions of the same task.

To start the ``celerybeat`` service::

    $ celerybeat

You can also start ``celerybeat`` with ``celeryd`` by using the ``-B`` option,
this is convenient if you only have one server::

    $ celeryd -B
