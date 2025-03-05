"""
This is a use case to test the pull request.

Usage::

    Start a rabbitmq, e.g.:

    $ docker run -d --hostname rabbit --name rabbit -p 15672:15672 -p 5672:5672 rabbitmq:3-management

    Install the virtualenv at requirements/default.txt

    (window 1)$ source /path/to/venv/bin/activate
    (window 1)$ PYTHONPATH="$(pwd)/../../" python -m celery.__main__ -A myapp worker -l INFO --concurrency 1 --prefetch-multiplier 1

    (window 2)$ source /path/to/venv/bin/activate
    (window 2)$ PYTHONPATH="$(pwd)/../../" python
    >>> from myapp import add
    >>> from myapp import block
    >>> block.s(100_000).apply_async(countdown=10, task_id="id-1/block")
    >>> add.s(1, 1).apply_async(countdown=10, task_id="id-2/add")
    >>> add.s(1, 1).apply_async(countdown=10, task_id="id-3/add")
    >>> add.s(1, 1).apply_async(countdown=10, task_id="id-4/add")
    >>> add.s(1, 1).apply_async(countdown=10, task_id="id-5/add")


You'll see how the worker rejects (requeues) the "id-1/block" after ETA is met,
and how RabbitMQ immediately pushes it back. This is because the worker still
has an idle worker able to process the task [1].

The rest of the tasks ("id-*/add"), are rejected (requeued) once their ETAs are met
but RabbitMQ don't push them back because this worker has a prefetch of 1 and
has 1 task reserved (`block` uses acks_late)


If you start a new worker, it will process the `add` tasks immediately.

   (window 3)$ source /path/to/venv/bin/activate
   (window 3)$ PYTHONPATH="$(pwd)/../../" python -m celery.__main__ -A myapp worker -l INFO --concurrency 1 --prefetch-multiplier 1

Without using this PR patch, the first worker keeps the `add` tasks for
itself until the `block` task is finished. The requeing could seem somewhat less
performant but blocking the other `add` tasks for 100_000 seconds would be
even worse.

To test this use case with the current version, just install it and run the same steps
without the PYTHONPATH prefix. You can switch between versions just adding or removing
the PYTHONPATH.


[1] RabbitMQ pushes a task if unacked_tasks < qos

  qos           = prefetch_multiplier * concurrency +
                  tasks_with_future_eta

  unacked_tasks = tasks_running_w_acks_late +
                  tasks_waiting +
                  tasks_with_future_eta

  tasks_with_future_eta     = tasks with ETA in the future         (not ready to run)
  tasks_waiting             = tasks with ETA in the past or no ETA (ready, wating for a worker)
  tasks_running             = tasks running, acked before start    (ready, running and acknowledged)
  tasks_running_w_acks_late = tasks running, acked before finished (ready, running and unacknowleged)
"""
import time

from celery import Celery

app = Celery(
    'myapp',
    broker='amqp://guest@localhost//',
)


@app.task(acks_late=True)
def add(x, y):
    return x + y

@app.task(bind=True, acks_late=True)
def block(self, seconds=3600):
    print(f"Started `block` task '{self.request.id}' that will sleep for {seconds} seconds")
    time.sleep(seconds)
    print(f"Ended `block` task '{self.request.id}' that has slept for {seconds} seconds")


if __name__ == '__main__':
    app.start()
