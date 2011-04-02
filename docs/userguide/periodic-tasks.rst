.. _guide-beat:

================
 Periodic Tasks
================

.. contents::
    :local:

Introduction
============

:program:`celerybeat` is a scheduler.  It kicks off tasks at regular intervals,
which are then executed by the worker nodes available in the cluster.

By default the entries are taken from the :setting:`CELERYBEAT_SCHEDULE` setting,
but custom stores can also be used, like storing the entries
in an SQL database.

You have to ensure only a single scheduler is running for a schedule
at a time, otherwise you would end up with duplicate tasks.  Using
a centralized approach means the schedule does not have to be synchronized,
and the service can operate without using locks.

.. _beat-entries:

Entries
=======

To schedule a task periodically you have to add an entry to the
:setting:`CELERYBEAT_SCHEDULE` setting.

Example: Run the `tasks.add` task every 30 seconds.

.. code-block:: python

    from datetime import timedelta

    CELERYBEAT_SCHEDULE = {
        "runs-every-30-seconds": {
            "task": "tasks.add",
            "schedule": timedelta(seconds=30),
            "args": (16, 16)
        },
    }


Using a :class:`~datetime.timedelta` for the schedule means the task will
be executed 30 seconds after `celerybeat` starts, and then every 30 seconds
after the last run.  A crontab like schedule also exists, see the section
on `Crontab schedules`_.

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
    relative to the time when :program:`celerybeat` was started.

.. _beat-crontab:

Crontab schedules
=================

If you want more control over when the task is executed, for
example, a particular time of day or day of the week, you can use
the `crontab` schedule type:

.. code-block:: python

    from celery.schedules import crontab

    CELERYBEAT_SCHEDULE = {
        # Executes every Monday morning at 7:30 A.M
        "every-monday-morning": {
            "task": "tasks.add",
            "schedule": crontab(hour=7, minute=30, day_of_week=1),
            "args": (16, 16),
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
| ``crontab(minute=0, hour="*/3")``       | Execute every three hours:                 |
|                                         | 3am, 6am, 9am, noon, 3pm, 6pm, 9pm.        |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0,``                   | Same as previous.                          |
|         ``hour=[0,3,6,9,12,15,18,21])`` |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute="*/15")``              | Execute every 15 minutes.                  |
+-----------------------------------------+--------------------------------------------+
| ``crontab(day_of_week="sunday")``       | Execute every minute (!) at Sundays.       |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute="*",``                 | Same as previous.                          |
|         ``hour="*",``                   |                                            |
|         ``day_of_week="sun")``          |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute="*/10",``              | Execute every ten minutes, but only        |
|         ``hour="3,17,22",``             | between 3-4 am, 5-6 pm and 10-11 pm on     |
|         ``day_of_week="thu,fri")``      | Thursdays or Fridays.                      |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0, hour="*/2,*/3")``   | Execute every even hour, and every hour    |
|                                         | divisible by three. This means:            |
|                                         | at every hour *except*: 1am,               |
|                                         | 5am, 7am, 11am, 1pm, 5pm, 7pm,             |
|                                         | 11pm                                       |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0, hour="*/5")``       | Execute hour divisible by 5. This means    |
|                                         | that it is triggered at 3pm, not 5pm       |
|                                         | (since 3pm equals the 24-hour clock        |
|                                         | value of "15", which is divisible by 5).   |
+-----------------------------------------+--------------------------------------------+
| ``crontab(minute=0, hour="*/3,8-17")``  | Execute every hour divisible by 3, and     |
|                                         | every hour during office hours (8am-5pm).  |
+-----------------------------------------+--------------------------------------------+

.. _beat-starting:

Starting celerybeat
===================

To start the :program:`celerybeat` service::

    $ celerybeat

You can also start `celerybeat` with `celeryd` by using the `-B` option,
this is convenient if you only intend to use one worker node::

    $ celeryd -B

Celerybeat needs to store the last run times of the tasks in a local database
file (named `celerybeat-schedule` by default), so it needs access to
write in the current directory, or alternatively you can specify a custom
location for this file::

    $ celerybeat -s /home/celery/var/run/celerybeat-schedule


.. note::

    To daemonize celerybeat see :ref:`daemonizing`.

.. _beat-custom-schedulers:

Using custom scheduler classes
------------------------------

Custom scheduler classes can be specified on the command line (the `-S`
argument).  The default scheduler is :class:`celery.beat.PersistentScheduler`,
which is simply keeping track of the last run times in a local database file
(a :mod:`shelve`).

`django-celery` also ships with a scheduler that stores the schedule in the
Django database::

    $ celerybeat -S djcelery.schedulers.DatabaseScheduler

Using `django-celery`'s scheduler you can add, modify and remove periodic
tasks from the Django Admin.
