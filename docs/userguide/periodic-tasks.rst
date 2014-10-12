.. _guide-beat:

================
 Periodic Tasks
================

.. contents::
    :local:

Introduction
============

:program:`celery beat` is a scheduler.  It kicks off tasks at regular intervals,
which are then executed by the worker nodes available in the cluster.

By default the entries are taken from the :setting:`CELERYBEAT_SCHEDULE` setting,
but custom stores can also be used, like storing the entries
in an SQL database.

You have to ensure only a single scheduler is running for a schedule
at a time, otherwise you would end up with duplicate tasks.  Using
a centralized approach means the schedule does not have to be synchronized,
and the service can operate without using locks.

.. _beat-timezones:

Time Zones
==========

The periodic task schedules uses the UTC time zone by default,
but you can change the time zone used using the :setting:`CELERY_TIMEZONE`
setting.

An example time zone could be `Europe/London`:

.. code-block:: python

    CELERY_TIMEZONE = 'Europe/London'


This setting must be added to your app, either by configuration it directly
using (``app.conf.CELERY_TIMEZONE = 'Europe/London'``), or by adding
it to your configuration module if you have set one up using
``app.config_from_object``.  See :ref:`celerytut-configuration` for
more information about configuration options.


The default scheduler (storing the schedule in the :file:`celerybeat-schedule`
file) will automatically detect that the time zone has changed, and so will
reset the schedule itself, but other schedulers may not be so smart (e.g. the
Django database scheduler, see below) and in that case you will have to reset the
schedule manually.

.. admonition:: Django Users

    Celery recommends and is compatible with the new ``USE_TZ`` setting introduced
    in Django 1.4.

    For Django users the time zone specified in the ``TIME_ZONE`` setting
    will be used, or you can specify a custom time zone for Celery alone
    by using the :setting:`CELERY_TIMEZONE` setting.

    The database scheduler will not reset when timezone related settings
    change, so you must do this manually:

    .. code-block:: bash

        $ python manage.py shell
        >>> from djcelery.models import PeriodicTask
        >>> PeriodicTask.objects.update(last_run_at=None)

.. _beat-entries:

Entries
=======

To schedule a task periodically you have to add an entry to the
:setting:`CELERYBEAT_SCHEDULE` setting.

Example: Run the `tasks.add` task every 30 seconds.

.. code-block:: python

    from datetime import timedelta

    CELERYBEAT_SCHEDULE = {
        'add-every-30-seconds': {
            'task': 'tasks.add',
            'schedule': timedelta(seconds=30),
            'args': (16, 16)
        },
    }

    CELERY_TIMEZONE = 'UTC'


.. note::

    If you are wondering where these settings should go then
    please see :ref:`celerytut-configuration`.  You can either
    set these options on your app directly or you can keep
    a separate module for configuration.

Using a :class:`~datetime.timedelta` for the schedule means the task will
be sent in 30 second intervals (the first task will be sent 30 seconds
after `celery beat` starts, and then every 30 seconds
after the last run).

A crontab like schedule also exists, see the section on `Crontab schedules`_.

Like with ``cron``, the tasks may overlap if the first task does not complete
before the next.  If that is a concern you should use a locking
strategy to ensure only one instance can run at a time (see for example
:ref:`cookbook-task-serial`).

.. _beat-entry-fields:

Available Fields
----------------

* `task`

    The name of the task to execute.

* `schedule`

    The frequency of execution.

    This can be the number of seconds as an integer, a
    :class:`~datetime.timedelta`, or a :class:`~celery.schedules.crontab`.
    You can also define your own custom schedule types, by extending the
    interface of :class:`~celery.schedules.schedule`.

* `args`

    Positional arguments (:class:`list` or :class:`tuple`).

* `kwargs`

    Keyword arguments (:class:`dict`).

* `options`

    Execution options (:class:`dict`).

    This can be any argument supported by
    :meth:`~celery.task.base.Task.apply_async`,
    e.g. `exchange`, `routing_key`, `expires`, and so on.

* `relative`

    By default :class:`~datetime.timedelta` schedules are scheduled
    "by the clock". This means the frequency is rounded to the nearest
    second, minute, hour or day depending on the period of the timedelta.

    If `relative` is true the frequency is not rounded and will be
    relative to the time when :program:`celery beat` was started.

.. _beat-crontab:

Crontab schedules
=================

If you want more control over when the task is executed, for
example, a particular time of day or day of the week, you can use
the :class:`~celery.schedules.crontab` schedule type:

.. code-block:: python

    from celery.schedules import crontab

    CELERYBEAT_SCHEDULE = {
        # Executes every Monday morning at 7:30 A.M
        'add-every-monday-morning': {
            'task': 'tasks.add',
            'schedule': crontab(hour=7, minute=30, day_of_week=1),
            'args': (16, 16),
        },
    }

The syntax of these crontab expressions are very flexible.  Some examples:

+-----------------------------------------+--------------------------------------------+
| **Example**                             | **Meaning**                                |
+-----------------------------------------+--------------------------------------------+
| ``crontab()``                           | Execute every minute.                      |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0, hour=0)``           | Execute daily at midnight.                 |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0, hour='*/3')``       | Execute every three hours:                 |
|                                         | midnight, 3am, 6am, 9am,                   |
|                                         | noon, 3pm, 6pm, 9pm.                       |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0,``                   | Same as previous.                          |
|         ``hour='0,3,6,9,12,15,18,21')`` |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute='*/15')``              | Execute every 15 minutes.                  |
+-----------------------------------------+--------------------------------------------+
| ``crontab(day_of_week='sunday')``       | Execute every minute (!) at Sundays.       |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute='*',``                 | Same as previous.                          |
|         ``hour='*',``                   |                                            |
|         ``day_of_week='sun')``          |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute='*/10',``              | Execute every ten minutes, but only        |
|         ``hour='3,17,22',``             | between 3-4 am, 5-6 pm and 10-11 pm on     |
|         ``day_of_week='thu,fri')``      | Thursdays or Fridays.                      |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0, hour='*/2,*/3')``   | Execute every even hour, and every hour    |
|                                         | divisible by three. This means:            |
|                                         | at every hour *except*: 1am,               |
|                                         | 5am, 7am, 11am, 1pm, 5pm, 7pm,             |
|                                         | 11pm                                       |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0, hour='*/5')``       | Execute hour divisible by 5. This means    |
|                                         | that it is triggered at 3pm, not 5pm       |
|                                         | (since 3pm equals the 24-hour clock        |
|                                         | value of "15", which is divisible by 5).   |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0, hour='*/3,8-17')``  | Execute every hour divisible by 3, and     |
|                                         | every hour during office hours (8am-5pm).  |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0, 0, day_of_month='2')``  | Execute on the second day of every month.  |
|                                         |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0, 0,'``                   | Execute on every even numbered day.        |
|         ``day_of_month='2-30/3')``      |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0, 0,``                    | Execute on the first and third weeks of    |
|         ``day_of_month='1-7,15-21')``   | the month.                                 |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0, 0, day_of_month='11',`` | Execute on 11th of May every year.         |
|          ``month_of_year='5')``         |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0, 0,``                    | Execute on the first month of every        |
|         ``month_of_year='*/3')``        | quarter.                                   |
+-----------------------------------------+--------------------------------------------+

See :class:`celery.schedules.crontab` for more documentation.

.. _beat-starting:

Starting the Scheduler
======================

To start the :program:`celery beat` service:

.. code-block:: bash

    $ celery -A proj beat

You can also start embed `beat` inside the worker by enabling
workers `-B` option, this is convenient if you will never run
more than one worker node, but it's not commonly used and for that
reason is not recommended for production use:

.. code-block:: bash

    $ celery -A proj worker -B

Beat needs to store the last run times of the tasks in a local database
file (named `celerybeat-schedule` by default), so it needs access to
write in the current directory, or alternatively you can specify a custom
location for this file:

.. code-block:: bash

    $ celery -A proj beat -s /home/celery/var/run/celerybeat-schedule


.. note::

    To daemonize beat see :ref:`daemonizing`.

.. _beat-custom-schedulers:

Using custom scheduler classes
------------------------------

Custom scheduler classes can be specified on the command-line (the `-S`
argument).  The default scheduler is :class:`celery.beat.PersistentScheduler`,
which is simply keeping track of the last run times in a local database file
(a :mod:`shelve`).

`django-celery` also ships with a scheduler that stores the schedule in the
Django database:

.. code-block:: bash

    $ celery -A proj beat -S djcelery.schedulers.DatabaseScheduler

Using `django-celery`'s scheduler you can add, modify and remove periodic
tasks from the Django Admin.
