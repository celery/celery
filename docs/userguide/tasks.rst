.. _guide-tasks:

=======
 Tasks
=======

.. contents::
    :local:


This guide gives an overview of how tasks are defined. For a complete
listing of task attributes and methods, please see the
:class:`API reference <celery.task.base.BaseTask>`.

.. _task-basics:

Basics
======

A task is a class that encapsulates a function and its execution options.
Given a function create_user`, that takes two arguments: `username` and
`password`, you can create a task like this:

.. code-block:: python

    from django.contrib.auth import User

    @task
    def create_user(username, password):
        User.objects.create(username=username, password=password)


Task options are added as arguments to `task`:

.. code-block:: python

    @task(serializer="json")
    def create_user(username, password):
        User.objects.create(username=username, password=password)

.. _task-request-info:

Context
=======

`task.request` contains information and state related
the currently executing task, and must always contain the following
attributes:

:id: The unique id of the executing task.

:taskset: The unique id of the taskset this task is a member of (if any).

:args: Positional arguments.

:kwargs: Keyword arguments.

:retries: How many times the current task has been retried.
          An integer starting at `0`.

:is_eager: Set to :const:`True` if the task is executed locally in
           the client, and not by a worker.

:logfile: The file the worker logs to.  See `Logging`_.

:loglevel: The current log level used.

:delivery_info: Additional message delivery information. This is a mapping
                containing the exchange and routing key used to deliver this
                task.  Used by e.g. :meth:`~celery.task.base.BaseTask.retry`
                to resend the task to the same destination queue.

  **NOTE** As some messaging backends doesn't have advanced routing
  capabilities, you can't trust the availability of keys in this mapping.


Example Usage
-------------

::

    @task
    def add(x, y):
        print("Executing task id %r, args: %r kwargs: %r" % (
            add.request.id, add.request.args, add.request.kwargs))

.. _task-logging:

Logging
=======

You can use the workers logger to add diagnostic output to
the worker log:

.. code-block:: python

    @task
    def add(x, y):
        logger = add.get_logger()
        logger.info("Adding %s + %s" % (x, y))
        return x + y

There are several logging levels available, and the workers `loglevel`
setting decides whether or not they will be written to the log file.

Of course, you can also simply use `print` as anything written to standard
out/-err will be written to the log file as well.

.. _task-retry:

Retrying a task if something fails
==================================

Simply use :meth:`~celery.task.base.BaseTask.retry` to re-send the task.
It will do the right thing, and respect the
:attr:`~celery.task.base.BaseTask.max_retries` attribute:

.. code-block:: python

    @task
    def send_twitter_status(oauth, tweet):
        try:
            twitter = Twitter(oauth)
            twitter.update_status(tweet)
        except (Twitter.FailWhaleError, Twitter.LoginError), exc:
            send_twitter_status.retry(exc=exc)

Here we used the `exc` argument to pass the current exception to
:meth:`~celery.task.base.BaseTask.retry`. At each step of the retry this exception
is available as the tombstone (result) of the task. When
:attr:`~celery.task.base.BaseTask.max_retries` has been exceeded this is the
exception raised.  However, if an `exc` argument is not provided the
:exc:`~celery.exceptions.RetryTaskError` exception is raised instead.

.. _task-retry-custom-delay:

Using a custom retry delay
--------------------------

When a task is to be retried, it will wait for a given amount of time
before doing so. The default delay is in the
:attr:`~celery.task.base.BaseTask.default_retry_delay` 
attribute on the task. By default this is set to 3 minutes. Note that the
unit for setting the delay is in seconds (int or float).

You can also provide the `countdown` argument to
:meth:`~celery.task.base.BaseTask.retry` to override this default.

.. code-block:: python

    @task(default_retry_delay=30 * 60)  # retry in 30 minutes.
    def add(x, y):
        try:
            ...
        except Exception, exc:
            self.retry(exc=exc, countdown=60)  # override the default and
                                               # retry in 1 minute

.. _task-options:

Task options
============

General
-------

.. _task-general-options:

.. attribute:: Task.name

    The name the task is registered as.

    You can set this name manually, or just use the default which is
    automatically generated using the module and class name.  See
    :ref:`task-names`.

.. attribute Task.request

    If the task is being executed this will contain information
    about the current request.  Thread local storage is used.

    See :ref:`task-request-info`.

.. attribute:: Task.abstract

    Abstract classes are not registered, but are used as the
    base class for new task types.

.. attribute:: Task.max_retries

    The maximum number of attempted retries before giving up.
    If this exceeds the :exc:`~celery.exceptions.MaxRetriesExceeded`
    an exception will be raised.  *NOTE:* You have to :meth:`retry`
    manually, it's not something that happens automatically.

.. attribute:: Task.default_retry_delay

    Default time in seconds before a retry of the task
    should be executed.  Can be either :class:`int` or :class:`float`.
    Default is a 3 minute delay.

.. attribute:: Task.rate_limit

    Set the rate limit for this task type, i.e. how many times in
    a given period of time is the task allowed to run.

    If this is :const:`None` no rate limit is in effect.
    If it is an integer, it is interpreted as "tasks per second". 

    The rate limits can be specified in seconds, minutes or hours
    by appending `"/s"`, `"/m"` or `"/h"` to the value.
    Example: `"100/m"` (hundred tasks a minute).  Default is the
    :setting:`CELERY_DEFAULT_RATE_LIMIT` setting, which if not specified means
    rate limiting for tasks is disabled by default.

.. attribute:: Task.ignore_result

    Don't store task state.    Note that this means you can't use
    :class:`~celery.result.AsyncResult` to check if the task is ready,
    or get its return value.

.. attribute:: Task.store_errors_even_if_ignored

    If :const:`True`, errors will be stored even if the task is configured
    to ignore results.

.. attribute:: Task.send_error_emails

    Send an e-mail whenever a task of this type fails.
    Defaults to the :setting:`CELERY_SEND_TASK_ERROR_EMAILS` setting.
    See :ref:`conf-error-mails` for more information.

.. attribute:: Task.error_whitelist

    If the sending of error e-mails is enabled for this task, then
    this is a white list of exceptions to actually send e-mails about.

.. attribute:: Task.serializer

    A string identifying the default serialization
    method to use. Defaults to the :setting:`CELERY_TASK_SERIALIZER`
    setting.  Can be `pickle` `json`, `yaml`, or any custom
    serialization methods that have been registered with
    :mod:`kombu.serialization.registry`.

    Please see :ref:`executing-serializers` for more information.

.. attribute:: Task.backend

    The result store backend to use for this task.  Defaults to the
    :setting:`CELERY_RESULT_BACKEND` setting.

.. attribute:: Task.acks_late

    If set to :const:`True` messages for this task will be acknowledged
    **after** the task has been executed, not *just before*, which is
    the default behavior.

    Note that this means the task may be executed twice if the worker
    crashes in the middle of execution, which may be acceptable for some
    applications.

    The global default can be overridden by the :setting:`CELERY_ACKS_LATE`
    setting.

.. attribute:: Task.track_started

    If :const:`True` the task will report its status as "started"
    when the task is executed by a worker.
    The default value is :const:`False` as the normal behaviour is to not
    report that level of granularity. Tasks are either pending, finished,
    or waiting to be retried.  Having a "started" status can be useful for
    when there are long running tasks and there is a need to report which
    task is currently running.

    The host name and process id of the worker executing the task
    will be available in the state metadata (e.g. `result.info["pid"]`)

    The global default can be overridden by the
    :setting:`CELERY_TRACK_STARTED` setting.


.. seealso::

    The API reference for :class:`~celery.task.base.BaseTask`.

.. _task-message-options:

Message and routing options
---------------------------

.. attribute:: Task.queue

    Use the routing settings from a queue defined in :setting:`CELERY_QUEUES`.
    If defined the :attr:`exchange` and :attr:`routing_key` options will be
    ignored.

.. attribute:: Task.exchange

    Override the global default `exchange` for this task.

.. attribute:: Task.routing_key

    Override the global default `routing_key` for this task.

.. attribute:: Task.mandatory

    If set, the task message has mandatory routing.  By default the task
    is silently dropped by the broker if it can't be routed to a queue.
    However -- If the task is mandatory, an exception will be raised
    instead.

    Not supported by amqplib.

.. attribute:: Task.immediate

    Request immediate delivery.  If the task cannot be routed to a
    task worker immediately, an exception will be raised.  This is
    instead of the default behavior, where the broker will accept and
    queue the task, but with no guarantee that the task will ever
    be executed.

    Not supported by amqplib.

.. attribute:: Task.priority

    The message priority. A number from 0 to 9, where 0 is the
    highest priority.

    Not supported by RabbitMQ.

.. seealso::

    :ref:`executing-routing` for more information about message options,
    and :ref:`guide-routing`.

.. _task-names:

Task names
==========

The task type is identified by the *task name*.

If not provided a name will be automatically generated using the module
and class name.

For example:

.. code-block:: python

    >>> @task(name="sum-of-two-numbers")
    >>> def add(x, y):
    ...     return x + y

    >>> add.name
    'sum-of-two-numbers'

The best practice is to use the module name as a prefix to classify the
tasks using namespaces.  This way the name won't collide with the name from
another module:

.. code-block:: python

    >>> @task(name="tasks.add")
    >>> def add(x, y):
    ...     return x + y

    >>> add.name
    'tasks.add'


Which is exactly the name that is automatically generated for this
task if the module name is "tasks.py":

.. code-block:: python

    >>> @task()
    >>> def add(x, y):
    ...     return x + y

    >>> add.name
    'tasks.add'

.. _task-naming-relative-imports:

Automatic naming and relative imports
-------------------------------------

Relative imports and automatic name generation does not go well together,
so if you're using relative imports you should set the name explicitly.

For example if the client imports the module "myapp.tasks" as ".tasks", and
the worker imports the module as "myapp.tasks", the generated names won't match
and an :exc:`~celery.exceptions.NotRegistered` error will be raised by the worker.

This is also the case if using Django and using `project.myapp`::

    INSTALLED_APPS = ("project.myapp", )

The worker will have the tasks registered as "project.myapp.tasks.*", 
while this is what happens in the client if the module is imported as
"myapp.tasks":

.. code-block:: python

    >>> from myapp.tasks import add
    >>> add.name
    'myapp.tasks.add'

For this reason you should never use "project.app", but rather
add the project directory to the Python path::

    import os
    import sys
    sys.path.append(os.getcwd())

    INSTALLED_APPS = ("myapp", )

This makes more sense from the reusable app perspective anyway.

.. _tasks-decorating:

Decorating tasks
================

When using other decorators you must make sure that the `task`
decorator is applied last:

.. code-block:: python

    @task
    @decorator2
    @decorator1
    def add(x, y):
        return x + y


Which means the `@task` decorator must be the top statement.

.. _task-states:

Task States
===========

Celery can keep track of the tasks current state.  The state also contains the
result of a successful task, or the exception and traceback information of a
failed task.

There are several *result backends* to choose from, and they all have
different strenghts and weaknesses (see :ref:`task-result-backends`).

During its lifetime a task will transition through several possible states,
and each state may have arbitrary metadata attached to it.  When a task
moves into a new state the previous state is
forgotten about, but some transitions can be deducted, (e.g. a task now
in the :state:`FAILED` state, is implied to have been in the
:state:`STARTED` state at some point).

There are also sets of states, like the set of
:state:`failure states <FAILURE_STATES>`, and the set of
:state:`ready states <READY_STATES>`.

The client uses the membership of these sets to decide whether
the exception should be re-raised (:state:`PROPAGATE_STATES`), or whether
the result can be cached (it can if the task is ready).

You can also define :ref:`custom-states`.

.. _task-result-backends:

Result Backends
---------------

Celery needs to store or send the states somewhere.  There are several
built-in backends to choose from: SQLAlchemy/Django ORM, Memcached, Redis,
AMQP, MongoDB, Tokyo Tyrant and Redis -- or you can define your own.

There is no backend that works well for every single use case, but for
historical reasons the default backend is the AMQP backend.  You should read
about the strenghts and weaknesses of each backend, and choose the most
appropriate for your own needs.


.. seealso::

    :ref:`conf-result-backend`

AMQP Result Backend
~~~~~~~~~~~~~~~~~~~

The AMQP result backend is special as it does not actually *store* the states,
but rather sends them as messages.  This is an important difference as it
means that a result *can only be retrieved once*; If you have two processes
waiting for the same result, one of the processes will never receive the
result!

Even with that limitation, it is an excellent choice if you need to receive
state changes in real-time.  Using messaging means the client does not have to
poll for new states.

There are several other pitfalls you should be aware of when using the AMQP
backend:

* Every new task creates a new queue on the server, with thousands of tasks
  the broker may be overloaded with queues and this will affect performance in
  negative ways. If you're using RabbitMQ then each queue will be a separate
  Erlang process, so if you're planning to keep many results simultaneously you
  may have to increase the Erlang process limit, and the maximum number of file
  descriptors your OS allows.

* Old results will not be cleaned automatically, so you must make sure to
  consume the results or else the number of queues will eventually go out of
  control.  If you're running RabbitMQ 2.1.1 or higher you can take advantage
  of the ``x-expires`` argument to queues, which will expire queues after a
  certain time limit after they are unused.  The queue expiry can be set (in
  seconds) by the :setting:`CELERY_AMQP_TASK_RESULT_EXPIRES` setting (not
  enabled by default).

For a list of options supported by the AMQP result backend, please see
:ref:`conf-amqp-result-backend`.


Database Result Backend
~~~~~~~~~~~~~~~~~~~~~~~

Keeping state in the database can be convenient for many, especially for
web applications with a database already in place, but it also comes with
limitations.

* Polling the database for new states is expensive, and so you should
  increase the polling intervals of operations such as `result.wait()`, and
  `tasksetresult.join()`

* Some databases uses a default transaction isolation level that
  is not suitable for polling tables for changes.

  In MySQL the default transaction isolation level is `REPEATABLE-READ`, which
  means the transaction will not see changes by other transactions until the
  transaction is commited.  It is recommended that you change to the
  `READ-COMMITTED` isolation level.


.. _task-builtin-states:

Built-in States
---------------

.. state:: PENDING

PENDING
~~~~~~~

Task is waiting for execution or unknown.
Any task id that is not know is implied to be in the pending state.

.. state:: STARTED

STARTED
~~~~~~~

Task has been started.
Not reported by default, to enable please see :attr`Task.track_started`.

:metadata: `pid` and `hostname` of the worker process executing
           the task.

.. state:: SUCCESS

SUCCESS
~~~~~~~

Task has been successfully executed.

:metadata: `result` contains the return value of the task.
:propagates: Yes
:ready: Yes

.. state:: FAILURE

FAILURE
~~~~~~~

Task execution resulted in failure.

:metadata: `result` contains the exception occurred, and `traceback`
           contains the backtrace of the stack at the point when the
           exception was raised.
:propagates: Yes

.. state:: RETRY

RETRY
~~~~~

Task is being retried.

:metadata: `result` contains the exception that caused the retry,
           and `traceback` contains the backtrace of the stack at the point
           when the exceptions was raised.
:propagates: No

.. state:: REVOKED

REVOKED
~~~~~~~

Task has been revoked.

:propagates: Yes

.. _custom-states:

Custom states
-------------

You can easily define your own states, all you need is a unique name.
The name of the state is usually an uppercase string.  As an example
you could have a look at :mod:`abortable tasks <~celery.contrib.abortable>`
which defines its own custom :state:`ABORTED` state.

Use :meth:`Task.update_state <celery.task.base.BaseTask.update_state>` to
update a tasks state::

    @task
    def upload_files(filenames):
        for i, file in enumerate(filenames):
            upload_files.update_state(state="PROGRESS",
                meta={"current": i, "total": len(filenames)})


Here we created the state `"PROGRESS"`, which tells any application
aware of this state that the task is currently in progress, and also where
it is in the process by having `current` and `total` counts as part of the
state metadata.  This can then be used to create e.g. progress bars.

.. _task-how-they-work:

How it works
============

Here comes the technical details, this part isn't something you need to know,
but you may be interested.

All defined tasks are listed in a registry.  The registry contains
a list of task names and their task classes.  You can investigate this registry
yourself:

.. code-block:: python

    >>> from celery import registry
    >>> from celery import task
    >>> registry.tasks
    {'celery.delete_expired_task_meta':
        <PeriodicTask: celery.delete_expired_task_meta (periodic)>,
     'celery.task.http.HttpDispatchTask':
        <Task: celery.task.http.HttpDispatchTask (regular)>,
     'celery.execute_remote':
        <Task: celery.execute_remote (regular)>,
     'celery.map_async':
        <Task: celery.map_async (regular)>,
     'celery.ping':
        <Task: celery.ping (regular)>}

This is the list of tasks built-in to celery.  Note that we had to import
`celery.task` first for these to show up.  This is because the tasks will
only be registered when the module they are defined in is imported.

The default loader imports any modules listed in the
:setting:`CELERY_IMPORTS` setting.

The entity responsible for registering your task in the registry is a
meta class, :class:`~celery.task.base.TaskType`.  This is the default
meta class for :class:`~celery.task.base.BaseTask`.

If you want to register your task manually you can mark the
task as :attr:`~celery.task.base.BaseTask.abstract`:

.. code-block:: python

    class MyTask(Task):
        abstract = True

This way the task won't be registered, but any task inheriting from
it will be.

When tasks are sent, we don't send any actual function code, just the name
of the task to execute.  When the worker then receives the message it can look
up the name in its task registry to find the execution code.

This means that your workers should always be updated with the same software
as the client.  This is a drawback, but the alternative is a technical
challenge that has yet to be solved.

.. _task-best-practices:

Tips and Best Practices
=======================

.. _task-ignore_results:

Ignore results you don't want
-----------------------------

If you don't care about the results of a task, be sure to set the
:attr:`~celery.task.base.BaseTask.ignore_result` option, as storing results
wastes time and resources.

.. code-block:: python

    @task(ignore_result=True)
    def mytask(...)
        something()

Results can even be disabled globally using the :setting:`CELERY_IGNORE_RESULT`
setting.

.. _task-disable-rate-limits:

Disable rate limits if they're not used
---------------------------------------

Disabling rate limits altogether is recommended if you don't have
any tasks using them.  This is because the rate limit subsystem introduces
quite a lot of complexity.

Set the :setting:`CELERY_DISABLE_RATE_LIMITS` setting to globally disable
rate limits:

.. code-block:: python

    CELERY_DISABLE_RATE_LIMITS = True

.. _task-synchronous-subtasks:

Avoid launching synchronous subtasks
------------------------------------

Having a task wait for the result of another task is really inefficient,
and may even cause a deadlock if the worker pool is exhausted.

Make your design asynchronous instead, for example by using *callbacks*.

**Bad**:

.. code-block:: python

    @task
    def update_page_info(url):
        page = fetch_page.delay(url).get()
        info = parse_page.delay(url, page).get()
        store_page_info.delay(url, info)

    @task
    def fetch_page(url):
        return myhttplib.get(url)

    @task
    def parse_page(url, page):
        return myparser.parse_document(page)

    @task
    def store_page_info(url, info):
        return PageInfo.objects.create(url, info)


**Good**:

.. code-block:: python

    @task(ignore_result=True)
    def update_page_info(url):
        # fetch_page -> parse_page -> store_page
        fetch_page.delay(url, callback=subtask(parse_page,
                                    callback=subtask(store_page_info)))

    @task(ignore_result=True)
    def fetch_page(url, callback=None):
        page = myhttplib.get(url)
        if callback:
            # The callback may have been serialized with JSON,
            # so best practice is to convert the subtask dict back
            # into a subtask object.
            subtask(callback).delay(url, page)

    @task(ignore_result=True)
    def parse_page(url, page, callback=None):
        info = myparser.parse_document(page)
        if callback:
            subtask(callback).delay(url, info)

    @task(ignore_result=True)
    def store_page_info(url, info):
        PageInfo.objects.create(url, info)


We use :class:`~celery.task.sets.subtask` here to safely pass
around the callback task.  :class:`~celery.task.sets.subtask` is a
subclass of dict used to wrap the arguments and execution options
for a single task invocation.


.. seealso::

    :ref:`sets-subtasks` for more information about subtasks.

.. _task-performance-and-strategies:

Performance and Strategies
==========================

.. _task-granularity:

Granularity
-----------

The task granularity is the amount of computation needed by each subtask.
In general it is better to split the problem up into many small tasks, than
have a few long running tasks.

With smaller tasks you can process more tasks in parallel and the tasks
won't run long enough to block the worker from processing other waiting tasks.

However, executing a task does have overhead. A message needs to be sent, data
may not be local, etc. So if the tasks are too fine-grained the additional
overhead may not be worth it in the end.

.. seealso::

    The book `Art of Concurrency`_ has a whole section dedicated to the topic
    of task granularity.

.. _`Art of Concurrency`: http://oreilly.com/catalog/9780596521547

.. _task-data-locality:

Data locality
-------------

The worker processing the task should be as close to the data as
possible.  The best would be to have a copy in memory, the worst would be a
full transfer from another continent.

If the data is far away, you could try to run another worker at location, or
if that's not possible - cache often used data, or preload data you know
is going to be used.

The easiest way to share data between workers is to use a distributed cache
system, like `memcached`_.

.. seealso::

    The paper `Distributed Computing Economics`_ by Jim Gray is an excellent
    introduction to the topic of data locality.

.. _`Distributed Computing Economics`:
    http://research.microsoft.com/pubs/70001/tr-2003-24.pdf

.. _`memcached`: http://memcached.org/

.. _task-state:

State
-----

Since celery is a distributed system, you can't know in which process, or
on what machine the task will be executed.  You can't even know if the task will
run in a timely manner.

The ancient async sayings tells us that “asserting the world is the
responsibility of the task”.  What this means is that the world view may
have changed since the task was requested, so the task is responsible for
making sure the world is how it should be;  If you have a task
that re-indexes a search engine, and the search engine should only be
re-indexed at maximum every 5 minutes, then it must be the tasks
responsibility to assert that, not the callers.

Another gotcha is Django model objects.  They shouldn't be passed on as
arguments to tasks.  It's almost always better to re-fetch the object from
the database when the task is running instead,  as using old data may lead
to race conditions.

Imagine the following scenario where you have an article and a task
that automatically expands some abbreviations in it:

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

Now, the queue is very busy, so the task won't be run for another 2 minutes.
In the meantime another author makes changes to the article, so
when the task is finally run, the body of the article is reverted to the old
version because the task had the old body in its argument.

Fixing the race condition is easy, just use the article id instead, and
re-fetch the article in the task body:

.. code-block:: python

    @task
    def expand_abbreviations(article_id):
        article = Article.objects.get(id=article_id)
        article.body.replace("MyCorp", "My Corporation")
        article.save()

    >>> expand_abbreviations(article_id)

There might even be performance benefits to this approach, as sending large
messages may be expensive.

.. _task-database-transactions:

Database transactions
---------------------

Let's have a look at another example:

.. code-block:: python

    from django.db import transaction

    @transaction.commit_on_success
    def create_article(request):
        article = Article.objects.create(....)
        expand_abbreviations.delay(article.pk)

This is a Django view creating an article object in the database,
then passing the primary key to a task.  It uses the `commit_on_success`
decorator, which will commit the transaction when the view returns, or
roll back if the view raises an exception.

There is a race condition if the task starts executing
before the transaction has been committed; The database object does not exist
yet!

The solution is to *always commit transactions before sending tasks
depending on state from the current transaction*:

.. code-block:: python

    @transaction.commit_manually
    def create_article(request):
        try:
            article = Article.objects.create(...)
        except:
            transaction.rollback()
            raise
        else:
            transaction.commit()
            expand_abbreviations.delay(article.pk)

.. _task-example:

Example
=======

Let's take a real wold example; A blog where comments posted needs to be
filtered for spam.  When the comment is created, the spam filter runs in the
background, so the user doesn't have to wait for it to finish.

We have a Django blog application allowing comments
on blog posts.  We'll describe parts of the models/views and tasks for this
application.

blog/models.py
--------------

The comment model looks like this:

.. code-block:: python

    from django.db import models
    from django.utils.translation import ugettext_lazy as _


    class Comment(models.Model):
        name = models.CharField(_("name"), max_length=64)
        email_address = models.EmailField(_("e-mail address"))
        homepage = models.URLField(_("home page"),
                                   blank=True, verify_exists=False)
        comment = models.TextField(_("comment"))
        pub_date = models.DateTimeField(_("Published date"),
                                        editable=False, auto_add_now=True)
        is_spam = models.BooleanField(_("spam?"),
                                      default=False, editable=False)

        class Meta:
            verbose_name = _("comment")
            verbose_name_plural = _("comments")


In the view where the comment is posted, we first write the comment
to the database, then we launch the spam filter task in the background.

.. _task-example-blog-views:

blog/views.py
-------------

.. code-block:: python

    from django import forms
    from django.http import HttpResponseRedirect
    from django.template.context import RequestContext
    from django.shortcuts import get_object_or_404, render_to_response

    from blog import tasks
    from blog.models import Comment


    class CommentForm(forms.ModelForm):

        class Meta:
            model = Comment


    def add_comment(request, slug, template_name="comments/create.html"):
        post = get_object_or_404(Entry, slug=slug)
        remote_addr = request.META.get("REMOTE_ADDR")

        if request.method == "post":
            form = CommentForm(request.POST, request.FILES)
            if form.is_valid():
                comment = form.save()
                # Check spam asynchronously.
                tasks.spam_filter.delay(comment_id=comment.id,
                                        remote_addr=remote_addr)
                return HttpResponseRedirect(post.get_absolute_url())
        else:
            form = CommentForm()

        context = RequestContext(request, {"form": form})
        return render_to_response(template_name, context_instance=context)


To filter spam in comments we use `Akismet`_, the service
used to filter spam in comments posted to the free weblog platform
`Wordpress`.  `Akismet`_ is free for personal use, but for commercial use you
need to pay.  You have to sign up to their service to get an API key.

To make API calls to `Akismet`_ we use the `akismet.py`_ library written by
`Michael Foord`_.

.. _task-example-blog-tasks:

blog/tasks.py
-------------

.. code-block:: python

    from akismet import Akismet
    from celery.task import task

    from django.core.exceptions import ImproperlyConfigured
    from django.contrib.sites.models import Site

    from blog.models import Comment


    @task
    def spam_filter(comment_id, remote_addr=None):
        logger = spam_filter.get_logger()
        logger.info("Running spam filter for comment %s" % comment_id)

        comment = Comment.objects.get(pk=comment_id)
        current_domain = Site.objects.get_current().domain
        akismet = Akismet(settings.AKISMET_KEY, "http://%s" % domain)
        if not akismet.verify_key():
            raise ImproperlyConfigured("Invalid AKISMET_KEY")


        is_spam = akismet.comment_check(user_ip=remote_addr,
                            comment_content=comment.comment,
                            comment_author=comment.name,
                            comment_author_email=comment.email_address)
        if is_spam:
            comment.is_spam = True
            comment.save()

        return is_spam

.. _`Akismet`: http://akismet.com/faq/
.. _`akismet.py`: http://www.voidspace.org.uk/downloads/akismet.py
.. _`Michael Foord`: http://www.voidspace.org.uk/
