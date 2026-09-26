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

    Celery recommends and is compatible with the ``USE_TZ`` setting introduced
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
    def setup_periodic_tasks(sender: Celery, **kwargs):
        # Calls test('hello') every 10 seconds.
        sender.add_periodic_task(10.0, test.s('hello'), name='add every 10')

        # Calls test('hello') every 30 seconds.
        # It uses the same signature of previous task, an explicit name is
        # defined to avoid this task replacing the previous one defined.
        sender.add_periodic_task(30.0, test.s('hello'), name='add every 30')

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

    @app.task
    def add(x, y):
        z = x + y
        print(z)



Setting these up from within the :data:`~@on_after_configure` handler means
that we'll not evaluate the app at module level when using ``test.s()``. Note that
:data:`~@on_after_configure` is sent after the app is set up, so tasks outside the
module where the app is declared (e.g. in a `tasks.py` file located by
:meth:`celery.Celery.autodiscover_tasks`) must use a later signal, such as
:data:`~@on_after_finalize`.

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

.. _beat-groups-workflows:

Scheduling groups and other workflows
-------------------------------------

:command:`beat` schedules a single task by name, so you can't pass a
:ref:`group <canvas-group>`, chain, or chord signature directly to
:meth:`~@add_periodic_task` (or a :setting:`beat_schedule` entry) -- an entry only
stores a task name with its arguments, not a workflow.

To run a workflow periodically, wrap it in a regular task and schedule that task:

.. code-block:: python

    from celery import Celery, group

    app = Celery()

    @app.task
    def add(x, y):
        return x + y

    @app.task
    def run_add_group():
        group(add.s(i, i) for i in range(10)).apply_async()

    @app.on_after_configure.connect
    def setup_periodic_tasks(sender: Celery, **kwargs):
        sender.add_periodic_task(30.0, run_add_group.s(), name='add group every 30')

The wrapper only *dispatches* the workflow with ``apply_async()`` and returns. Don't
call ``get()`` on the result inside the task to wait for it to finish: blocking on a
result from within a task ties up a worker process and is discouraged (see
:ref:`task-synchronous-subtasks`). The group's tasks run independently on the workers,
so the initiating task can return immediately.

.. _beat-entry-fields:

Available Fields
----------------

* `task`

    The name of the task to execute.

    Task names are described in the :ref:`task-names` section of the User Guide.
    Note that this is not the import path of the task, even though the default
    naming pattern is built like it is.

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
    :meth:`~celery.app.task.Task.apply_async` --
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
:class:`~celery.schedules.solar` schedule type.

Solar schedules require the :pypi:`ephem` library, so
to use them you must install Celery with the ``solar`` extra:

.. code-block:: console

    $ pip install celery[solar]

Example:

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

.. _beat-health-checks:

Health checks
-------------

.. versionadded:: 5.7

By default there's no way to check whether a running beat process is
still healthy. If you enable the
:setting:`beat_enable_remote_control` setting -- or pass
:option:`--enable-remote-control <celery beat --enable-remote-control>`
-- beat joins the same remote-control exchange the workers use (as a
node named ``celerybeat@hostname``) and answers
:program:`celery inspect ping`:

.. code-block:: console

    $ celery -A proj inspect ping -t 5 -d celerybeat@$(hostname)
    ->  celerybeat@example.com: OK
            pong

The command exits non-zero when nobody replies within the timeout.
Give :option:`--timeout <celery inspect --timeout>` room to spare: it
defaults to one second, and the probe has to establish a broker
connection of its own before it can ask anything.

Beat's reply is the same ``{'ok': 'pong'}`` a worker sends, so nothing
downstream has to special-case it.

What a reply means
~~~~~~~~~~~~~~~~~~

The control node runs in its own thread, so on its own a reply would
only prove that the process is alive and reaching the broker -- not
that the schedule is advancing. To close that gap, beat stops answering
once the scheduler has not completed a pass for
:setting:`beat_remote_control_max_tick_age` seconds, which defaults to
twice the interval the scheduler settled on. A wedged scheduler
therefore fails the probe rather than passing it.

Silence is deliberate: :program:`celery inspect` exits non-zero only
when *no* node replies, so an error reply would leave the exit status
at zero and the probe green. Beat logs a warning each time it declines,
so the reason is visible in its own output.

Set :setting:`beat_remote_control_max_tick_age` to ``0`` to answer
regardless of tick age.

Choosing a probe
~~~~~~~~~~~~~~~~

Only a **liveness** probe actually recovers a wedged beat: beat serves
no traffic and sits behind no Service, so marking a pod NotReady
removes nothing and starts no remediation. A readiness probe on beat
buys you visibility in ``kubectl get pods`` and gating for Deployment
rollouts -- useful, but it will not restart anything.

The catch is that this check travels over the broker, so it fails
whenever the broker is unreachable -- during an ordinary broker
restart, and for every beat pod at once. Restarting beat does not fix a
broker outage, and a beat that restarts re-reads its schedule. Wired
carelessly, a short blip becomes a cluster-wide restart storm.

So use a liveness probe, but give it a ``failureThreshold`` that rides
out a broker restart and an ``initialDelaySeconds`` long enough for the
control node to have connected, or the first probe kills a healthy pod:

.. code-block:: yaml

    livenessProbe:
      exec:
        command:
          - /bin/sh
          - -c
          - celery -A proj inspect ping -t 10 -d celerybeat@$(hostname)
      initialDelaySeconds: 60
      periodSeconds: 60
      failureThreshold: 5

With those numbers a wedged beat is restarted after about five
minutes, while a broker restart has to last that long before it costs
you anything.

Add a readiness probe as well if you want the state surfaced in
``kubectl`` and rollouts gated on beat coming up:

.. code-block:: yaml

    readinessProbe:
      exec:
        command:
          - /bin/sh
          - -c
          - celery -A proj inspect ping -t 5 -d celerybeat@$(hostname)
      initialDelaySeconds: 30
      periodSeconds: 60
      failureThreshold: 3

Comparison with a heartbeat file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Projects such as `django-celery-beat`_ and Nautobot take a different
approach: the scheduler touches a file on each tick, and the probe
checks its age. That has no broker dependency at all, so it cannot be
taken down by a broker outage, and it proves the tick loop directly.

The trade-off is reach. A file is only visible from inside the
container, so it answers "is *this* beat alive" and nothing more.
Remote control answers the same question from anywhere that can talk to
the broker, which is what lets ``celery status`` and centralised
monitoring see beat alongside the workers. Pick the file if you only
need a local probe; pick remote control if you want beat visible in the
same place as everything else.

.. _django-celery-beat:
    https://github.com/celery/django-celery-beat

Interactions worth knowing
~~~~~~~~~~~~~~~~~~~~~~~~~~

* Beat shows up as a node in destination-less
  :program:`celery inspect ping` and :program:`celery status` output
  once this is enabled. If you count nodes in those replies, the count
  changes.
* For the same reason, a destination-less broadcast with a reply limit
  -- ``app.control.ping(limit=1)``, say -- may now come back with beat
  instead of a worker.
* Beat implements ``ping`` and nothing else. Other remote-control
  commands are ignored silently, ``shutdown`` included: ``celery
  control shutdown`` stops your workers and leaves beat running.
* A beat scheduler embedded in a worker
  (:option:`-B <celery worker -B>`) never starts a control node, since
  the worker already answers for that process.
* Node names have to be unique. Two beats resolving to the same name --
  the same hostname, or containers on ``network_mode: host`` -- share
  one pidbox queue, so only one of them ever answers. Give each its own
  :option:`--hostname <celery beat --hostname>` if that can happen.
* Remote control needs fanout exchanges, so it is available on the
  RabbitMQ (AMQP) and Redis transports. On a transport without them
  beat logs one warning at startup and carries on without a control
  node.

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

        $ celery -A proj beat -l INFO --scheduler django_celery_beat.schedulers:DatabaseScheduler

   Note:  You may also add this as the :setting:`beat_scheduler` setting directly.

#. Visit the Django-Admin interface to set up some periodic tasks.
