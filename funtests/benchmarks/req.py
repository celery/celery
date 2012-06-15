from celery import current_app, task, uuid
from celery.worker.consumer import Consumer
from celery.worker.job import Request
from celery.concurrency.solo import TaskPool
from celery.app.amqp import TASK_BARE
from time import time
from Queue import Queue
from librabbitmq import Message
import socket
import sys

@task(accept_magic_kwargs=False)
def T():
    pass

tid = uuid()
P = TaskPool()
hostname = socket.gethostname()
task = {'task': T.name, 'args': (), 'kwargs': {}, 'id': tid, 'flags': 0}
app = current_app._get_current_object()
ready_queue = Queue()

def on_put(req):
    req.execute_using_pool(P)

def on_ack(*a): pass


m = Message(None, {}, {}, task)

ready_queue.put = on_put
x = Consumer(ready_queue, hostname=hostname, app=app)
x.update_strategies()
name = T.name
ts = time()
for i in xrange(100000):
    x.strategies[name](m, m.body, on_ack)
print(time() - ts)

