================
 Periodic Tasks
================

.. contents::
    :local:

Introduction
============

The :mod:`~celery.bin.celerybeat` service enables you to schedule tasks to
run at intervals.

Periodic tasks are defined as special task classes.
Here's an example of a periodic task:

.. code-block:: python

    from celery.decorators import periodic_task
    from datetime import timedelta

    @periodic_task(run_every=timedelta(seconds=30))
    def every_30_seconds():
        print("Running periodic task!")

Crontab-like schedules
======================

If you want a little more control over when the task is executed, for
example, a particular time of day or day of the week, you can use
the ``crontab`` schedule type:

.. code-block:: python

    from celery.task.schedules import crontab
    from celery.decorators import periodic_task

    @periodic_task(run_every=crontab(hour=7, minute=30, day_of_week=1))
    def every_monday_morning():
        print("Execute every Monday at 7:30AM.")

The syntax of these crontab expressions is very flexible.  Some examples:

+-------------------------------------+--------------------------------------------+
| **Example**                         | **Meaning**                                |
+-------------------------------------+--------------------------------------------+
| crontab()                           | Execute every minute.                      |
+-------------------------------------+--------------------------------------------+
| crontab(minute=0, hour=0)           | Execute daily at midnight.                 |
+-------------------------------------+--------------------------------------------+
| crontab(minute=0,                   | Execute every three hours---at midnight,   |
|                                     | 3am, 6am, 9am, noon, 3pm, 6pm, 9pm.        |
+-------------------------------------+--------------------------------------------+
| crontab(minute=0,                   | Same as previous.                          |
|         hour=[0,3,6,9,12,15,18,21]) |                                            |
+-------------------------------------+--------------------------------------------+
| crontab(minute="\*/15")             | Execute every 15 minutes.                  |
+-------------------------------------+--------------------------------------------+
| crontab(day_of_week="sunday")       | Execute every minute (!) at sundays.       |
+-------------------------------------+--------------------------------------------+
| crontab(minute="*",                 | Same as previous.                          |
|         hour="*",                   |                                            |
|         day_of_week="sun")          |                                            |
+-------------------------------------+--------------------------------------------+
| crontab(minute="\*/10",             | Execute every ten minutes, but only        |
|         hour="3,17,22",             | between 3-4 am, 5-6 pm and 10-11 pm on     |
|         day_of_week="thu,fri")      | thursdays or fridays.                      |
+-------------------------------------+--------------------------------------------+
| crontab(minute=0, hour="\*/2,\*/3") | Execute every even hour, and every hour    |
|                                     | divisable by three. This means:            |
|                                     | at every hour *except*: 1am,               |
|                                     | 5am, 7am, 11am, 1pm, 5pm, 7pm,             |
|                                     | 11pm                                       |
+-------------------------------------+--------------------------------------------+
| crontab(minute=0, hour="\*/5")      | Execute hour divisable by 5. This means    |
|                                     | that it is triggered at 3pm, not 5pm       |
|                                     | (since 3pm equals the 24-hour clock        |
|                                     | value of "15", which is divisable by 5).   |
+-------------------------------------+--------------------------------------------+
| crontab(minute=0, hour="\*/3,8-17") | Execute every hour divisable by 3, and     |
|                                     | every hour during office hours (8am-5pm).  |
+-------------------------------------+--------------------------------------------+

Starting celerybeat
===================

If you want to use periodic tasks you need to start the ``celerybeat``
service. You have to make sure only one instance of this server is running at
any time, or else you will end up with multiple executions of the same task.

To start the ``celerybeat`` service::

    $ celerybeat

You can also start ``celerybeat`` with ``celeryd`` by using the ``-B`` option,
this is convenient if you only have one server::

    $ celeryd -B
