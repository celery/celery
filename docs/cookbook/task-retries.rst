================
 Retrying Tasks
================


Retrying a task if something fails
==================================

Simply use :meth:`celery.task.base.Task.retry` to re-sent the task, it will
do the right thing, and respect the :attr:`celery.task.base.Task.max_retries`
attribute:

.. code-block:: python

    class SendTwitterStatusTask(Task):
    
        def run(self, oauth, tweet, **kwargs):
            try:
                twitter = Twitter(oauth)
                twitter.update_status(tweet)
            except (Twitter.FailWhaleError, Twitter.LoginError), exc:
                self.retry(args=[oauth, tweet], exc=exc, **kwargs)

Here we used the ``exc`` argument to pass the current exception to
:meth:`celery.task.base.Task.retry`. At each step of the retry this exception
is available as the tombstone (result) of the task, when
:attr:`celery.task.base.Task.max_retries` has been exceeded this is the exception
raised. However, if an ``exc`` argument is not provided the
:exc:`celery.task.base.RetryTaskError` exception is raised instead.
  
Setting a custom delay for retries.
===================================

The default countdown is in the tasks
:attr:`celery.task.base.Task.default_retry_delay` attribute, which by
default is set to 3 minutes.

You can also provide the ``countdown`` argument to
:meth:`celery.task.base.Task.retry` to override this default.

.. code-block:: python

    class MyTask(Task):
        default_retry_delay = 30 * 60 # retry in 30 minutes

        def run(self, x, y, **kwargs):
            try:
                ...
            except Exception, exc:
                self.retry([x, y], exc=exc,
                           countdown=60 # override the default and
                                        # retry in 1 minute
                           **kwargs)

