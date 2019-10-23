.. _guide-beat:

================
 Periodic Tasks
================

.. contents::
    :local:

Introduction
============

:program:`celery beat` is a scheduler; It kicks off tasks at regular intervals,
that are then executed by available worker nodes in the cluster.

By default the entries are taken from the :setting:`beat_schedule` setting,
but custom stores can also be used, like storing the entries in a SQL database.

You have to ensure only a single scheduler is running for a schedule
at a time, otherwise you'd end up with duplicate tasks. Using
a centralized approach means the schedule doesn't have to be synchronized,
and the service can operate without using locks.

.. _beat-timezones:

Time Zones
==========

The periodic task schedules uses the UTC time zone by default,
but you can change the time zone used using the :setting:`timezone`
setting.

An example time zone could be `Europe/London`:

.. code-block:: python

    timezone = 'Europe/London'

This setting must be added to your app, either by configuring it directly
using (``app.conf.timezone = 'Europe/London'``), or by adding
it to your configuration module if you have set one up using
``app.config_from_object``. See :ref:`celerytut-configuration` for
more information about configuration options.

The default scheduler (storing the schedule in the :file:`celerybeat-schedule`
file) will automatically detect that the time zone has changed, and so will
reset the schedule itself, but other schedulers may not be so smart (e.g., the
Django database scheduler, see below) and in that case you'll have to reset the
schedule manually.

.. admonition:: Django Users

    Celery recommends and is compatible with the new ``USE_TZ`` setting introduced
    in Django 1.4.

    For Django users the time zone specified in the ``TIME_ZONE`` setting
    will be used, or you can specify a custom time zone for Celery alone
    by using the :setting:`timezone` setting.

    The database scheduler won't reset when timezone related settings
    change, so you must do this manually:

    .. code-block:: console

        $ python manage.py shell
        >>> from djcelery.models import PeriodicTask
        >>> PeriodicTask.objects.update(last_run_at=None)

    Django-Celery only supports Celery 4.0 and below, for Celery 4.0 and above, do as follow:

    .. code-block:: console

        $ python manage.py shell
        >>> from django_celery_beat.models import PeriodicTask
        >>> PeriodicTask.objects.update(last_run_at=None)

.. _beat-entries:

Entries
=======

To call a task periodically you have to add an entry to the
beat schedule list.

.. code-block:: python

    from celery import Celery
    from celery.schedules import crontab

    app = Celery()

    @app.on_after_configure.connect
    def setup_periodic_tasks(sender, **kwargs):
        # Calls test('hello') every 10 seconds.
        sender.add_periodic_task(10.0, test.s('hello'), name='add every 10')

        # Calls test('world') every 30 seconds
        sender.add_periodic_task(30.0, test.s('world'), expires=10)

        # Executes every Monday morning at 7:30 a.m.
        sender.add_periodic_task(
            crontab(hour=7, minute=30, day_of_week=1),
            test.s('Happy Mondays!'),
        )

    @app.task
    def test(arg):
        print(arg)


Setting these up from within the :data:`~@on_after_configure` handler means
that we'll not evaluate the app at module level when using ``test.s()``.

The :meth:`~@add_periodic_task` function will add the entry to the
:setting:`beat_schedule` setting behind the scenes, and the same setting
can also be used to set up periodic tasks manually:

Example: Run the `tasks.add` task every 30 seconds.

.. code-block:: python

    app.conf.beat_schedule = {
        'add-every-30-seconds': {
            'task': 'tasks.add',
            'schedule': 30.0,
            'args': (16, 16)
        },
    }
    app.conf.timezone = 'UTC'


.. note::

    If you're wondering where these settings should go then
    please see :ref:`celerytut-configuration`. You can either
    set these options on your app directly or you can keep
    a separate module for configuration.

    If you want to use a single item tuple for `args`, don't forget
    that the constructor is a comma, and not a pair of parentheses.

Using a :class:`~datetime.timedelta` for the schedule means the task will
be sent in 30 second intervals (the first task will be sent 30 seconds
after `celery beat` starts, and then every 30 seconds
after the last run).

A Crontab like schedule also exists, see the section on `Crontab schedules`_.

Like with :command:`cron`, the tasks may overlap if the first task doesn't complete
before the next. If that's a concern you should use a locking
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
    :meth:`~celery.task.base.Task.apply_async` --
    `exchange`, `routing_key`, `expires`, and so on.

* `relative`

    If `relative` is true :class:`~datetime.timedelta` schedules are scheduled
    "by the clock." This means the frequency is rounded to the nearest
    second, minute, hour or day depending on the period of the
    :class:`~datetime.timedelta`.

    By default `relative` is false, the frequency isn't rounded and will be
    relative to the time when :program:`celery beat` was started.

.. _beat-crontab:

Crontab schedules
=================

If you want more control over when the task is executed, for
example, a particular time of day or day of the week, you can use
the :class:`~celery.schedules.crontab` schedule type:

.. code-block:: python

    from celery.schedules import crontab

    app.conf.beat_schedule = {
        # Executes every Monday morning at 7:30 a.m.
        'add-every-monday-morning': {
            'task': 'tasks.add',
            'schedule': crontab(hour=7, minute=30, day_of_week=1),
            'args': (16, 16),
        },
    }

The syntax of these Crontab expressions are very flexible.

Some examples:

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
|         ``hour='3,17,22',``             | between 3-4 am, 5-6 pm, and 10-11 pm on    |
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
| ``crontab(0, 0, day_of_month='2')``     | Execute on the second day of every month.  |
|                                         |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0,``                       | Execute on every even numbered day.        |
|         ``day_of_month='2-30/2')``      |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0,``                       | Execute on the first and third weeks of    |
|         ``day_of_month='1-7,15-21')``   | the month.                                 |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0, day_of_month='11',``    | Execute on the eleventh of May every year. |
|          ``month_of_year='5')``         |                                            |
+-----------------------------------------+--------------------------------------------+
| ``crontab(0, 0,``                       | Execute every day on the first month       |
|         ``month_of_year='*/3')``        | of every quarter.                          |
+-----------------------------------------+--------------------------------------------+

See :class:`celery.schedules.crontab` for more documentation.

.. _beat-solar:

Solar schedules
=================

If you have a task that should be executed according to sunrise,
sunset, dawn or dusk, you can use the
:class:`~celery.schedules.solar` schedule type:

.. code-block:: python

    from celery.schedules import solar

    app.conf.beat_schedule = {
        # Executes at sunset in Melbourne
        'add-at-melbourne-sunset': {
            'task': 'tasks.add',
            'schedule': solar('sunset', -37.81753, 144.96715),
            'args': (16, 16),
        },
    }

The arguments are simply: ``solar(event, latitude, longitude)``

Be sure to use the correct sign for latitude and longitude:

+---------------+-------------------+----------------------+
| **Sign**      | **Argument**      | **Meaning**          |
+---------------+-------------------+----------------------+
| ``+``         | ``latitude``      | North                |
+---------------+-------------------+----------------------+
| ``-``         | ``latitude``      | South                |
+---------------+-------------------+----------------------+
| ``+``         | ``longitude``     | East                 |
+---------------+-------------------+----------------------+
| ``-``         | ``longitude``     | West                 |
+---------------+-------------------+----------------------+

Possible event types are:

+-----------------------------------------+--------------------------------------------+
| **Event**                               | **Meaning**                                |
+-----------------------------------------+--------------------------------------------+
| ``dawn_astronomical``                   | Execute at the moment after which the sky  |
|                                         | is no longer completely dark. This is when |
|                                         | the sun is 18 degrees below the horizon.   |
+-----------------------------------------+--------------------------------------------+
| ``dawn_nautical``                       | Execute when there's enough sunlight for   |
|                                         | the horizon and some objects to be         |
|                                         | distinguishable; formally, when the sun is |
|                                         | 12 degrees below the horizon.              |
+-----------------------------------------+--------------------------------------------+
| ``dawn_civil``                          | Execute when there's enough light for      |
|                                         | objects to be distinguishable so that      |
|                                         | outdoor activities can commence;           |
|                                         | formally, when the Sun is 6 degrees below  |
|                                         | the horizon.                               |
+-----------------------------------------+--------------------------------------------+
| ``sunrise``                             | Execute when the upper edge of the sun     |
|                                         | appears over the eastern horizon in the    |
|                                         | morning.                                   |
+-----------------------------------------+--------------------------------------------+
| ``solar_noon``                          | Execute when the sun is highest above the  |
|                                         | horizon on that day.                       |
+-----------------------------------------+--------------------------------------------+
| ``sunset``                              | Execute when the trailing edge of the sun  |
|                                         | disappears over the western horizon in the |
|                                         | evening.                                   |
+-----------------------------------------+--------------------------------------------+
| ``dusk_civil``                          | Execute at the end of civil twilight, when |
|                                         | objects are still distinguishable and some |
|                                         | stars and planets are visible. Formally,   |
|                                         | when the sun is 6 degrees below the        |
|                                         | horizon.                                   |
+-----------------------------------------+--------------------------------------------+
| ``dusk_nautical``                       | Execute when the sun is 12 degrees below   |
|                                         | the horizon. Objects are no longer         |
|                                         | distinguishable, and the horizon is no     |
|                                         | longer visible to the naked eye.           |
+-----------------------------------------+--------------------------------------------+
| ``dusk_astronomical``                   | Execute at the moment after which the sky  |
|                                         | becomes completely dark; formally, when    |
|                                         | the sun is 18 degrees below the horizon.   |
+-----------------------------------------+--------------------------------------------+

All solar events are calculated using UTC, and are therefore
unaffected by your timezone setting.

In polar regions, the sun may not rise or set every day. The scheduler
is able to handle these cases (i.e., a ``sunrise`` event won't run on a day
when the sun doesn't rise). The one exception is ``solar_noon``, which is
formally defined as the moment the sun transits the celestial meridian,
and will occur every day even if the sun is below the horizon.

Twilight is defined as the period between dawn and sunrise; and between
sunset and dusk. You can schedule an event according to "twilight"
depending on your definition of twilight (civil, nautical, or astronomical),
and whether you want the event to take place at the beginning or end
of twilight, using the appropriate event from the list above.

See :class:`celery.schedules.solar` for more documentation.

.. _beat-starting:

Starting the Scheduler
======================

To start the :program:`celery beat` service:

.. code-block:: console

    $ celery -A proj beat

You can also embed `beat` inside the worker by enabling the
workers :option:`-B <celery worker -B>` option, this is convenient if you'll
never run more than one worker node, but it's not commonly used and for that
reason isn't recommended for production use:

.. code-block:: console

    $ celery -A proj worker -B

Beat needs to store the last run times of the tasks in a local database
file (named `celerybeat-schedule` by default), so it needs access to
write in the current directory, or alternatively you can specify a custom
location for this file:

.. code-block:: console

    $ celery -A proj beat -s /home/celery/var/run/celerybeat-schedule


.. note::

    To daemonize beat see :ref:`daemonizing`.

.. _beat-custom-schedulers:

Using custom scheduler classes
------------------------------

Custom scheduler classes can be specified on the command-line (the
:option:`--scheduler <celery beat --scheduler>` argument).

The default scheduler is the :class:`celery.beat.PersistentScheduler`,
that simply keeps track of the last run times in a local :mod:`shelve`
database file.

There's also the :pypi:`django-celery-beat` extension that stores the schedule
in the Django database, and presents a convenient admin interface to manage
periodic tasks at runtime.

To install and use this extension:

#. Use :command:`pip` to install the package:

    .. code-block:: console

        $ pip install django-celery-beat

#. Add the ``django_celery_beat`` module to ``INSTALLED_APPS`` in your
   Django project' :file:`settings.py`::

        INSTALLED_APPS = (
            ...,
            'django_celery_beat',
        )

   Note that there is no dash in the module name, only underscores.

#. Apply Django database migrations so that the necessary tables are created:

    .. code-block:: console

        $ python manage.py migrate

#. Start the :program:`celery beat` service using the ``django_celery_beat.schedulers:DatabaseScheduler`` scheduler:

    .. code-block:: console

        $ celery -A proj beat -l info --scheduler django_celery_beat.schedulers:DatabaseScheduler

   Note:  You may also add this as the :setting:`beat_scheduler` setting directly.

#. Visit the Django-Admin interface to set up some periodic tasks.
