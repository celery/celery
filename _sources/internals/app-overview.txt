=============================
 "The Big Instance" Refactor
=============================

The `app` branch is a work-in-progress to remove
the use of a global configuration in Celery.

Celery can now be instantiated, which means several
instances of Celery may exist in the same process space.
Also, large parts can be customized without resorting to monkey
patching.

Examples
========

Creating a Celery instance::

    >>> from celery import Celery
    >>> celery = Celery()
    >>> celery.config_from_object("celeryconfig")
    >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")


Creating tasks:

.. code-block:: python

    @celery.task()
    def add(x, y):
        return x + y


Creating custom Task subclasses:

.. code-block:: python

    Task = celery.create_task_cls()

    class DebugTask(Task):
        abstract = True

        def on_failure(self, \*args, \*\*kwargs):
            import pdb
            pdb.set_trace()

    @celery.task(base=DebugTask)
    def add(x, y):
        return x + y

Starting a worker:

.. code-block:: python

    worker = celery.Worker(loglevel="INFO")

Getting access to the configuration:

.. code-block:: python

    celery.conf.CELERY_ALWAYS_EAGER = True
    celery.conf["CELERY_ALWAYS_EAGER"] = True


Controlling workers::

    >>> celery.control.inspect().active()
    >>> celery.control.rate_limit(add.name, "100/m")
    >>> celery.control.broadcast("shutdown")
    >>> celery.control.discard_all()

Other interesting attributes::

    # Establish broker connection.
    >>> celery.broker_connection()

    # AMQP Specific features.
    >>> celery.amqp
    >>> celery.amqp.Router
    >>> celery.amqp.get_queues()
    >>> celery.amqp.get_task_consumer()

    # Loader
    >>> celery.loader

    # Default backend
    >>> celery.backend


As you can probably see, this really opens up another
dimension of customization abilities.

Deprecations
============

* celery.task.ping
  celery.task.PingTask

  Inferior to the ping remote control command.
  Will be removed in Celery 2.3.

Removed deprecations
====================

* `celery.utils.timedelta_seconds`
    Use: :func:`celery.utils.timeutils.timedelta_seconds`

* `celery.utils.defaultdict`
    Use: :func:`celery.utils.compat.defaultdict`

* `celery.utils.all`
    Use: :func:`celery.utils.compat.all`

* `celery.task.apply_async`
    Use app.send_task

* `celery.task.tasks`
    Use :data:`celery.registry.tasks`

Aliases (Pending deprecation)
=============================

* celery.task.base
    * .Task -> {app.create_task_cls}

* celery.task.sets
    * .TaskSet -> {app.TaskSet}

* celery.decorators / celery.task
    * .task -> {app.task}

* celery.execute
    * .apply_async -> {task.apply_async}
    * .apply -> {task.apply}
    * .send_task -> {app.send_task}
    * .delay_task -> no alternative

* celery.log
    * .get_default_logger -> {app.log.get_default_logger}
    * .setup_logger -> {app.log.setup_logger}
    * .get_task_logger -> {app.log.get_task_logger}
    * .setup_task_logger -> {app.log.setup_task_logger}
    * .setup_logging_subsystem -> {app.log.setup_logging_subsystem}
    * .redirect_stdouts_to_logger -> {app.log.redirect_stdouts_to_logger}

* celery.messaging
    * .establish_connection -> {app.broker_connection}
    * .with_connection -> {app.with_connection}
    * .get_consumer_set -> {app.amqp.get_task_consumer}
    * .TaskPublisher -> {app.amqp.TaskPublisher}
    * .TaskConsumer -> {app.amqp.TaskConsumer}
    * .ConsumerSet -> {app.amqp.ConsumerSet}

* celery.conf.* -> {app.conf}

    **NOTE**: All configuration keys are now named the same
    as in the configuration. So the key "CELERY_ALWAYS_EAGER"
    is accessed as::

        >>> app.conf.CELERY_ALWAYS_EAGER

    instead of::

        >>> from celery import conf
        >>> conf.ALWAYS_EAGER

    * .get_queues -> {app.amqp.get_queues}

* celery.task.control
    * .broadcast -> {app.control.broadcast}
    * .rate_limit -> {app.control.rate_limit}
    * .ping -> {app.control.ping}
    * .revoke -> {app.control.revoke}
    * .discard_all -> {app.control.discard_all}
    * .inspect -> {app.control.inspect}

* celery.utils.info
    * .humanize_seconds -> celery.utils.timeutils.humanize_seconds
    * .textindent -> celery.utils.textindent
    * .get_broker_info -> {app.amqp.get_broker_info}
    * .format_broker_info -> {app.amqp.format_broker_info}
    * .format_queues -> {app.amqp.format_queues}

Default App Usage
=================

To be backward compatible, it must be possible
to use all the classes/functions without passing
an explicit app instance.

This is achieved by having all app-dependent objects
use :data:`~celery.app.default_app` if the app instance
is missing.

.. code-block:: python

    from celery.app import app_or_default

    class SomeClass(object):

        def __init__(self, app=None):
            self.app = app_or_default(app)

The problem with this approach is that there is a chance
that the app instance is lost along the way, and everything
seems to be working normally. Testing app instance leaks
is hard. The environment variable :envvar:`CELERY_TRACE_APP`
can be used, when this is enabled :func:`celery.app.app_or_default`
will raise an exception whenever it has to go back to the default app
instance.

App Dependency Tree
-------------------

* {app}
    * celery.loaders.base.BaseLoader
    * celery.backends.base.BaseBackend
    * {app.TaskSet}
        * celery.task.sets.TaskSet (app.TaskSet)
    * [app.TaskSetResult]
        * celery.result.TaskSetResult (app.TaskSetResult)

* {app.AsyncResult}
    * celery.result.BaseAsyncResult / celery.result.AsyncResult

* celery.bin.celeryd.WorkerCommand
    * celery.apps.worker.Worker
        * celery.worker.WorkerController
            * celery.worker.consumer.Consumer
                * celery.worker.job.TaskRequest
                * celery.events.EventDispatcher
                * celery.worker.control.ControlDispatch
                    * celery.woker.control.registry.Panel
                    * celery.pidbox.BroadcastPublisher
                * celery.pidbox.BroadcastConsumer
            * celery.worker.controllers.Mediator
            * celery.beat.EmbeddedService

* celery.bin.celeryev.run_celeryev
    * celery.events.snapshot.evcam
        * celery.events.snapshot.Polaroid
        * celery.events.EventReceiver
    * celery.events.cursesmon.evtop
        * celery.events.EventReceiver
        * celery.events.cursesmon.CursesMonitor
    * celery.events.dumper
        * celery.events.EventReceiver

* celery.bin.celeryctl.celeryctl
    * celery.bin.celeryctl.Command

* celery.bin.caqmadm.AMQPAdmin

* celery.bin.celerybeat.BeatCommand
    * celery.apps.beat.Beat
        * celery.beat.Service
            * celery.beat.Scheduler

