"""The Celery application the due-work contract runs: a task that sends a message, its link, its error callback."""

import os

import redis

from celery import Celery

BROKER = os.environ.get('TEST_BROKER', 'redis://localhost:6379/10')
BACKEND = os.environ.get('TEST_BACKEND', 'redis://localhost:6379/11')
#: What the tasks did, readable from the test process and every worker process.
RECORDS = redis.Redis.from_url(os.environ.get('DUE_WORK_RECORDS', 'redis://localhost:6379/12'))

app = Celery('due_work', broker=BROKER, backend=BACKEND)
app.conf.update(task_acks_late=True, worker_prefetch_multiplier=1)


@app.task
def send_message(message):
    # EXTERNAL SEAM: the message leaves for the customer.
    RECORDS.rpush('sent', message)
    return message


@app.task
def announce(result):
    """The link: tell the customer it was sent."""
    RECORDS.rpush('announced', f'sent {result!r}')


@app.task
def report_failure(request, exc, traceback):
    """The error callback: tell the customer it failed."""
    RECORDS.rpush('announced', f'failed: {type(exc).__name__}')
