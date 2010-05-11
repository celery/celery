=======
 Tasks
=======

.. module:: celery.task.base

A task is a class that encapsulates a function and its execution options.
Given a function ``create_user``, that takes two arguments: ``username`` and
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
Tasks can choose not to take these, or list the ones they want.
The worker will do the right thing.

The current default keyword arguments are:

* logfile

    The log file, can be passed on to ``self.get_logger``
    to gain access to the workers log file. See `Logging`_.

* loglevel

    The loglevel used.

* task_id

    The unique id of the executing task.

* task_name

    Name of the executing task.

* task_retries

    How many times the current task has been retried.
    An integer starting at ``0``.

* task_is_eager

    Set to ``True`` if the task is executed locally in the client,
    and not by a worker.

* delivery_info

  Additional message delivery information. This is a mapping containing
  the exchange and routing key used to deliver this task. It's used
  by e.g. :meth:`retry` to resend the task to the same destination queue.

  **NOTE** As some messaging backends doesn't have advanced routing
  capabilities, you can't trust the availability of keys in this mapping.


Logging
=======

You can use the workers logger to add diagnostic output to
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
setting decides whether or not they will be written to the log file.


Retrying a task if something fails
==================================

Simply use :meth:`Task.retry` to re-send the task. It will
do the right thing, and respect the :attr:`Task.max_retries`
attribute:

.. code-block:: python

    @task()
    def send_twitter_status(oauth, tweet, **kwargs):
        try:
            twitter = Twitter(oauth)
            twitter.update_status(tweet)
        except (Twitter.FailWhaleError, Twitter.LoginError), exc:
            send_twitter_status.retry(args=[oauth, tweet], kwargs=kwargs, exc=exc)

Here we used the ``exc`` argument to pass the current exception to
:meth:`Task.retry`. At each step of the retry this exception
is available as the tombstone (result) of the task. When
:attr:`Task.max_retries` has been exceeded this is the exception
raised. However, if an ``exc`` argument is not provided the
:exc:`RetryTaskError` exception is raised instead.

**Important note:** The task has to take the magic keyword arguments
in order for max retries to work properly, this is because it keeps track
of the current number of retries using the ``task_retries`` keyword argument
passed on to the task. In addition, it also uses the ``task_id`` keyword
argument to use the same task id, and ``delivery_info`` to route the
retried task to the same destination.

Using a custom retry delay
--------------------------

When a task is to be retried, it will wait for a given amount of time
before doing so. The default delay is in the :attr:`Task.default_retry_delay` 
attribute on the task. By default this is set to 3 minutes. Note that the
unit for setting the delay is in seconds (int or float).

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

    The name the task is registered as.
    You can set this name manually, or just use the default which is
    automatically generated using the module and class name.

* abstract

    Abstract classes are not registered, but are used as the superclass
    when making new task types by subclassing.

* max_retries

    The maximum number of attempted retries before giving up.
    If this is exceeded the :exc`celery.execptions.MaxRetriesExceeded`
    exception will be raised. Note that you have to retry manually, it's
    not something that happens automatically.

* default_retry_delay

    Default time in seconds before a retry of the task should be
    executed. Can be either an ``int`` or a ``float``.
    Default is a 1 minute delay (``60 seconds``).

* rate_limit

  Set the rate limit for this task type, that is, how many times in a given
  period of time is the task allowed to run.

  If this is ``None`` no rate limit is in effect.
  If it is an integer, it is interpreted as "tasks per second". 

  The rate limits can be specified in seconds, minutes or hours
  by appending ``"/s"``, ``"/m"`` or "``/h"``" to the value.
  Example: ``"100/m" (hundred tasks a
  minute). Default is the ``CELERY_DEFAULT_RATE_LIMIT`` setting, which if not
  specified means rate limiting for tasks is turned off by default.

* ignore_result

  Don't store the status and return value. This means you can't
        use the :class:`celery.result.AsyncResult` to check if the task is
        done, or get its return value. Only use if you need the performance
        and is able live without these features. Any exceptions raised will
        store the return value/status as usual.

* disable_error_emails

    Disable error e-mails for this task. Default is ``False``.
    *Note:* You can also turn off error e-mails globally using the
    ``CELERY_SEND_TASK_ERROR_EMAILS`` setting.

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
    instead of the default behavior, where the broker will accept and
    queue the task, but with no guarantee that the task will ever
    be executed.

* priority
    The message priority. A number from ``0`` to ``9``, where ``0`` is the
    highest. **Note:** RabbitMQ does not support priorities yet.

See :doc:`executing` for more information about the messaging options
available.

Example
=======

Let's take a real wold example; A blog where comments posted needs to be
filtered for spam. When the comment is created, the spam filter runs in the
background, so the user doesn't have to wait for it to finish.

We have a Django blog application allowing comments
on blog posts. We'll describe parts of the models/views and tasks for this
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

blog/views.py
-------------

.. code-block:: python

    from django import forms
    frmo django.http import HttpResponseRedirect
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
`Wordpress`. `Akismet`_ is free for personal use, but for commercial use you
need to pay. You have to sign up to their service to get an API key.

To make API calls to `Akismet`_ we use the `akismet.py`_ library written by
Michael Foord.

blog/tasks.py
-------------

.. code-block:: python

    from akismet import Akismet
    from celery.decorators import task

    from django.core.exceptions import ImproperlyConfigured
    from django.contrib.sites.models import Site

    from blog.models import Comment


    @task
    def spam_filter(comment_id, remote_addr=None, **kwargs):
            logger = spam_filter.get_logger(**kwargs)
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

How it works
============

Here comes the technical details, this part isn't something you need to know,
but you may be interested.

All defined tasks are listed in a registry. The registry contains
a list of task names and their task classes. You can investigate this registry
yourself:

.. code-block:: python

    >>> from celery import registry
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
only be registered when the module they are defined in is imported.

The default loader imports any modules listed in the
``CELERY_IMPORTS`` setting. If using Django it loads all ``tasks.py`` modules
for the applications listed in ``INSTALLED_APPS``. If you want to do something
special you can create your own loader to do what you want.

The entity responsible for registering your task in the registry is a
meta class, :class:`TaskType`. This is the default meta class for
``Task``. If you want to register your task manually you can set the
``abstract`` attribute:

.. code-block:: python

    class MyTask(Task):
        abstract = True

This way the task won't be registered, but any task subclassing it will.

When tasks are sent, we don't send the function code, just the name
of the task. When the worker receives the message it can just look it up in
the task registry to find the execution code.

This means that your workers should always be updated with the same software
as the client. This is a drawback, but the alternative is a technical
challenge that has yet to be solved.

Tips and Best Practices
=======================

Ignore results you don't want
-----------------------------

If you don't care about the results of a task, be sure to set the
``ignore_result`` option, as storing results wastes time and resources.

.. code-block:: python

    @task(ignore_result=True)
    def mytask(...)
        something()

Results can even be disabled globally using the ``CELERY_IGNORE_RESULT``
setting.

Avoid launching synchronous subtasks
------------------------------------

Having a task wait for the result of another task is really inefficient,
and may even cause a deadlock if the worker pool is exhausted.

Make your design asynchronous instead, for example by using *callbacks*.


Bad:

.. code-block:: python

    @task()
    def update_page_info(url):
        page = fetch_page.delay(url).get()
        info = parse_page.delay(url, page).get()
        store_page_info.delay(url, info)

    @task()
    def fetch_page(url):
        return myhttplib.get(url)

    @task()
    def parse_page(url, page):
        return myparser.parse_document(page)

    @task()
    def store_page_info(url, info):
        return PageInfo.objects.create(url, info)


Good:

.. code-block:: python

    from functools import curry

    @task(ignore_result=True)
    def update_page_info(url):
        # fetch_page -> parse_page -> store_page
        callback = curry(parse_page.delay, callback=store_page_info)
        fetch_page.delay(url, callback=callback)

    @task(ignore_result=True)
    def fetch_page(url, callback=None):
        page = myparser.parse_document(page)
        if callback:
            callback(page)

    @task(ignore_result=True)
    def parse_page(url, page, callback=None):
        info = myparser.parse_document(page)
        if callback:
            callback(url, info)

    @task(ignore_result=True)
    def store_page_info(url, info):
        PageInfo.objects.create(url, info)





Performance and Strategies
==========================

Granularity
-----------

The task's granularity is the degree of parallelization your task have.
It's better to have many small tasks, than a few long running ones.

With smaller tasks, you can process more tasks in parallel and the tasks
won't run long enough to block the worker from processing other waiting tasks.

However, there's a limit. Sending messages takes processing power and bandwidth. If
your tasks are so short the overhead of passing them around is worse than
just executing them in-line, you should reconsider your strategy. There is no
universal answer here.

Data locality
-------------

The worker processing the task should be as close to the data as
possible. The best would be to have a copy in memory, the worst being a
full transfer from another continent.

If the data is far away, you could try to run another worker at location, or
if that's not possible, cache often used data, or preload data you know
is going to be used.

The easiest way to share data between workers is to use a distributed caching
system, like `memcached`_.

For more information about data-locality, please read
http://research.microsoft.com/pubs/70001/tr-2003-24.pdf

.. _`memcached`: http://memcached.org/


State
-----

Since celery is a distributed system, you can't know in which process, or even
on what machine the task will run. Indeed you can't even know if the task will
run in a timely manner, so please be wary of the state you pass on to tasks.

One gotcha is Django model objects. They shouldn't be passed on as arguments
to task classes, it's almost always better to re-fetch the object from the
database instead, as there are possible race conditions involved.

Imagine the following scenario where you have an article and a task
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
re-fetch the article in the task body:

.. code-block:: python

    @task
    def expand_abbreviations(article_id)
        article = Article.objects.get(id=article_id)
        article.body.replace("MyCorp", "My Corporation")
        article.save()

    >>> expand_abbreviations(article_id)

There might even be performance benefits to this approach, as sending large
messages may be expensive.
