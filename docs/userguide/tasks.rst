=======
 Tasks
=======

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

    @task()
    def create_user(username, password):
        User.objects.create(username=username, password=password)

Note the parens after ``@task()`` the task decorator takes any execution
options the ``Task`` class does:

.. code-block:: python

    @task(serializer="json")
    def create_user(username, password):
        User.objects.create(username=username, password=password)


An alternative way to use the decorator is to give the function as an argument
instead, but if you do this be sure to set the return values ``__name__``
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

    from celery.decorators import task
    @task()
    def add(x, y, **kwargs):
        logger = add.get_logger(**kwargs)
        logger.info("Adding %s + %s" % (x, y))
        return x + y

There are several logging levels available, and the workers ``loglevel``
setting decides whether they will be sent to the log file or not.
