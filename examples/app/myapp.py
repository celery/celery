"""myapp.py

Usage::

   (window1)$ python myapp.py worker -l INFO

   (window2)$ python
   >>> from myapp import add
   >>> add.delay(16, 16).get()
   32

You can also specify the app to use with the `celery` command,
using the `-A` / `--app` option::

    $ celery -A myapp worker -l INFO

With the `-A myproj` argument the program will search for an app
instance in the module ``myproj``.  You can also specify an explicit
name using the fully qualified form::

    $ celery -A myapp:app worker -l INFO

"""

import os
from time import sleep

from celery import Celery

os.environ["AWS_PROFILE"] = "localstack"


app = Celery(
    "myapp",
    broker="sqs://localhost:4566",
    backend="rpc://",
)

app.conf.update(
    worker_prefetch_multiplier=1,
    worker_concurrency=1,
    worker_heartbeat=None,
    broker_heartbeat=0,
    task_acks_late=True,
    task_reject_on_worker_lost=False,
    # worker_sqspoc=True, by default
    broker_transport_options={
        "region": "us-east-1",
        "visibility_timeout": 3600,
        "polling_interval": 1,
    },
)


@app.task(bind=True)
def add(self, x, y):
    return x + y


@app.task(bind=True)
def long_running_task(self, i=2000):
    print(f"long_running_task started, sleeping {i}s")
    sleep(i)
    # os.kill(os.getpid(), signal.SIGTERM)
    return f"done long_running_task (sleep {i}s)"


if __name__ == "__main__":
    app.start()
