=======
 Tasks
=======

.. module:: celery.task.base

A task is a class that encapsulates a function and its execution options.
With a function ``create_user``, that takes two arguments: ``username`` and
``password``, you can create a task like this:

.. code-block:: python

    from celery.task import Task

    class CreateUserTask(Task):
        def run(self, username, password):
            create_user(username, password)

For convenience there is a shortcut decorator that turns any function into
a task, ``celery.decorators.task``:

.. code-block:: python

    from celery.decorators import task
    from django.contrib.auth import User

    @task
    def create_user(username, password):
        User.objects.create(username=username, password=password)

The task decorator takes the same execution options the ``Task`` class does:

.. code-block:: python

    @task(serializer="json")
    def create_user(username, password):
        User.objects.create(username=username, password=password)


An alternative way to use the decorator is to give the function as an argument
instead, but if you do this be sure to set the resulting tasks ``__name__``
attribute, so pickle is able to find it in reverse:

.. code-block:: python

    create_user_task = task()(create_user)
    create_user_task.__name__ = "create_user_task"


Default keyword arguments
=========================

Celery supports a set of default arguments that can be forwarded to any task.
task can choose not to take these, or only list the ones it want
(the worker will do the right thing).

The current default keyword arguments are:

* logfile

    The currently used log file, can be passed on to ``self.get_logger``
    to gain access to the workers log file. See `Logging`_.

* loglevel

    The current loglevel used.

* task_id

    The unique id of the executing task.

* task_name

    Name of the executing task.

* task_retries

    How many times the current task has been retried.
    (an integer starting a ``0``).

Logging
=======

You can use the workers logger to add some diagnostic output to
the worker log:

.. code-block:: python

    class AddTask(Task):
        def run(self, x, y, **kwargs):
            logger = self.get_logger(**kwargs)
            logger.info("Adding %s + %s" % (x, y))
            return x + y

or using the decorator syntax:

.. code-block:: python

    @task()
    def add(x, y, **kwargs):
        logger = add.get_logger(**kwargs)
        logger.info("Adding %s + %s" % (x, y))
        return x + y

There are several logging levels available, and the workers ``loglevel``
setting decides whether they will be sent to the log file or not.

Retrying a task if something fails
==================================

Simply use :meth:`Task.retry` to re-sent the task, it will
do the right thing, and respect the :attr:`Task.max_retries`
attribute:

.. code-block:: python

    @task()
    def send_twitter_status(oauth, tweet, **kwargs):
        try:
            twitter = Twitter(oauth)
            twitter.update_status(tweet)
        except (Twitter.FailWhaleError, Twitter.LoginError), exc:
            send_twitter_status.retry(args=[oauth, tweet], kwargs, exc=exc)

Here we used the ``exc`` argument to pass the current exception to
:meth:`Task.retry`. At each step of the retry this exception
is available as the tombstone (result) of the task, when
:attr:`Task.max_retries` has been exceeded this is the exception
raised. However, if an ``exc`` argument is not provided the
:exc:`RetryTaskError` exception is raised instead.

Using a custom retry delay
--------------------------

The default countdown is in the tasks
:attr:`Task.default_retry_delay` attribute, which by
default is set to 3 minutes.

You can also provide the ``countdown`` argument to
:meth:`Task.retry` to override this default.

.. code-block:: python

    class MyTask(Task):
        default_retry_delay = 30 * 60 # retry in 30 minutes

        def run(self, x, y, **kwargs):
            try:
                ...
            except Exception, exc:
                self.retry([x, y], kwargs, exc=exc,
                           countdown=60) # override the default and
                                         # - retry in 1 minute



Task options
============

* name

    This is the name the task is registered as.
    You can set this name manually, or just use the default which is
    atomatically generated using the module and class name.

* abstract

    Abstract classes are not registered, so they're
    only used for making new task types by subclassing.

* max_retries

    The maximum number of attempted retries before giving up.
    If this is exceeded the :exc`celery.execptions.MaxRetriesExceeded`
    exception will be raised. Note that you have to retry manually, it's
    not something that happens automatically.

* default_retry_delay

    Default time in seconds before a retry of the task should be
    executed. Default is a 1 minute delay.

* rate_limit

  Set the rate limit for this task type,
  if this is ``None`` no rate limit is in effect.
  The rate limits can be specified in seconds, minutes or hours
  by appending ``"/s"``, ``"/m"`` or "``/h"``". If this is an integer
  it is interpreted as seconds. Example: ``"100/m" (hundred tasks a
  minute). Default is the ``CELERY_DEFAULT_RATE_LIMIT`` setting (which
  is off if not specified).

* ignore_result

  Don't store the status and return value. This means you can't
        use the :class:`celery.result.AsyncResult` to check if the task is
        done, or get its return value. Only use if you need the performance
        and is able live without these features. Any exceptions raised will
        store the return value/status as usual.

* disable_error_emails

    Disable all error e-mails for this task.

* serializer

    A string identifying the default serialization
    method to use. Defaults to the ``CELERY_TASK_SERIALIZER`` setting.
    Can be ``pickle`` ``json``, ``yaml``, or any custom serialization
    methods that have been registered with
    :mod:`carrot.serialization.registry`.

    Please see :doc:`executing` for more information.

Message and routing options
---------------------------

* routing_key
    Override the global default ``routing_key`` for this task.

* exchange
    Override the global default ``exchange`` for this task.

* mandatory
    If set, the task message has mandatory routing. By default the task
    is silently dropped by the broker if it can't be routed to a queue.
    However - If the task is mandatory, an exception will be raised
    instead.

* immediate
    Request immediate delivery. If the task cannot be routed to a
    task worker immediately, an exception will be raised. This is
    instead of the default behaviour, where the broker will accept and
    queue the task, but with no guarantee that the task will ever
    be executed.

* priority
    The message priority. A number from ``0`` to ``9``, where ``0`` is the
    highest. Note that RabbitMQ doesn't support priorities yet.

Please see :doc:`executing` for descriptions of these options.

How it works
============

Here comes the technical details, this part isn't something you need to know,
but you may be interested, so here goes.

All defined tasks are listed in a registry. The registry contains
a list of task names and their task classes. You can investigate this registry
by yourself:

.. code-block:: python

    >>> from celery.task import registry
    >>> from celery import task
    >>> registry.tasks
    {'celery.delete_expired_task_meta':
      <celery.task.builtins.DeleteExpiredTaskMetaTask object at 0x101d1f510>,
    'celery.execute_remote':
      <celery.task.base.ExecuteRemoteTask object at 0x101d17890>,
    'celery.task.rest.RESTProxyTask':
      <celery.task.rest.RESTProxyTask object at 0x101d1f410>,
    'celery.task.rest.Task': <celery.task.rest.Task object at 0x101d1f4d0>,
    'celery.map_async':
      <celery.task.base.AsynchronousMapTask object at 0x101d17910>,
    'celery.ping': <celery.task.builtins.PingTask object at 0x101d1f550>}

This is the list of tasks built-in to celery. Note that we had to import
``celery.task`` first for these to show up. This is because the tasks will
only be registered when the module it is defined in is imported.

When using the default loader the loader imports any modules listed in the
``CELERY_IMPORTS`` setting. If using Django it loads all ``tasks.py`` modules
for the applications listed in ``INSTALLED_APPS``. If you want to do something
special you can create your own loader to do what you want.

The entity responsible for registering your task in the registry is a
metaclass, :class:`TaskType`, this is the default metaclass for
``Task``. If you want to register your task manually you can set the
``abstract`` attribute:

.. code-block:: python

    class MyTask(Task):
        abstract = True

This way the task won't be registered, but any task subclassing it will.

So when we send a task, we don't send the function code, we just send the name
of the task, so when the worker receives the message it can just look it up in
the task registry to find the execution code.

This means that your workers must optimally be updated with the same software
as the client, this is a drawback, but the alternative is a technical
challenge that has yet to be solved.

Performance and Strategies
==========================

Granularity
-----------

The tasks granularity is the degree of parallelization your task have.
It's better to have a lot of small tasks, than just a few long running
ones.

With smaller tasks, you can process more tasks in parallel and the tasks
won't run long enough to block the worker from processing other waiting tasks.

But there's a limit, sending messages takes processing power and bandwidth. If
your tasks are so short the overhead of passing them around is worse than
just executing them inline, you should reconsider your strategy. There is no
universal answer here.

Data locality
-------------

The worker processing the task should optimally be as close to the data as
possible. The best would be to have a copy in memory, the worst being a
full transfer from another continent.

If the data is far away, you could try to run another worker at location, or
if that's not possible, cache often used data, or preload data you know
is going to be used.

The easiest way to share data between workers is to use a distributed caching
system, like `memcached`_.

.. _`memcached`: http://memcached.org/
http://research.microsoft.com/pubs/70001/tr-2003-24.pdf

State
-----

Since celery is a distributed system, you can't know in which process, or even
on what machine the task will run, also you can't even know if the task will
run in a timely manner, so please be wary of the state you pass on to tasks.

One gotcha is Django model objects, they shouldn't be passed on as arguments
to task classes, it's almost always better to refetch the object from the
database instead, as there are possible race conditions involved.

Imagine the following scenario where you have an article, and a task
that automatically expands some abbreviations in it.

.. code-block:: python

    class Article(models.Model):
        title = models.CharField()
        body = models.TextField()

    @task
    def expand_abbreviations(article):
        article.body.replace("MyCorp", "My Corporation")
        article.save()

First, an author creates an article and saves it, then the author
clicks on a button that initiates the abbreviation task.

    >>> article = Article.objects.get(id=102)
    >>> expand_abbreviations.delay(model_object)

Now, the queue is very busy, so the task won't be run for another 2 minutes,
in the meantime another author makes some changes to the article,
when the task is finally run, the body of the article is reverted to the old
version, because the task had the old body in its argument.

Fixing the race condition is easy, just use the article id instead, and
refetch the article in the task body:

.. code-block:: python

    @task
    def expand_abbreviations(article_id)
        article = Article.objects.get(id=article_id)
        article.body.replace("MyCorp", "My Corporation")
        article.save()

    >>> expand_abbreviations(article_id)

There might even be performance benefits to this approach, as sending large
messages may be expensive.
