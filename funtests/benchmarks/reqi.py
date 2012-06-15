from celery import current_app, task, uuid
from celery.worker.consumer import Consumer
#from celery.worker.job import Request
from celery.app.task import Context
from celery.concurrency.solo import TaskPool
from celery.app.amqp import TASK_BARE
from time import time
from Queue import Queue
from librabbitmq import Message
from celery.utils.functional import noop
from celery.worker.job import NEEDS_KWDICT
from celery.datastructures import AttributeDict
import socket
import sys

@task(accept_magic_kwargs=False)
def T():
    pass

class Request(object):
    #__slots__ = ('app', 'name', 'id', 'args', 'kwargs',
    #             'on_ack', 'delivery_info', 'hostname',
    #             'eventer', 'connection_errors',
    #             'task', 'eta', 'expires', 'flags',
    #             'request_dict', 'acknowledged',
    #             'worker_pid', 'started',
    #             '_already_revoked', '_terminate_on_ack', '_tzlocal')
    eta = None
    started = False
    acknowledged = _already_revoked = False
    worker_pid = _terminate_on_ack = None
    _tzlocal = None
    expires = None
    delivery_info = {}
    flags = 0
    args = ()

    def __init__(self, body, on_ack=noop,
            hostname=None, eventer=None, app=None,
            connection_errors=None, request_dict=None,
            delivery_info=None, task=None, Context=Context, **opts):
        self.app = app
        self.name = body['task']
        self.id = body['id']
        self.args = body['args']
        try:
            self.kwargs = body['kwargs']
            if NEEDS_KWDICT:
                self.kwargs = kwdict(self.kwargs)
        except KeyError:
            self.kwargs = {}
        try:
            self.flags = body['flags']
        except KeyError:
            pass
        self.on_ack = on_ack
        self.hostname = hostname
        self.eventer = eventer
        self.connection_errors = connection_errors or ()
        self.task = task or self.app._tasks[self.name]
        if 'eta' in body:
            eta = body['eta']
            tz = tz_utc if utc else self.tzlocal
            self.eta = tz_to_local(maybe_iso8601(eta), self.tzlocal, tz)
        if 'expires' in body:
            expires = body['expires']
            tz = tz_utc if utc else self.tzlocal
            self.expires = tz_to_local(maybe_iso8601(expires),
                                       self.tzlocal, tz)
        if delivery_info:
            self.delivery_info = {
                'exchange': delivery_info.get('exchange'),
                'routing_key': delivery_info.get('routing_key'),
            }

        self.request_dict = AttributeDict(
                {'called_directly': False,
                 'callbacks': [],
                 'errbacks': [],
                 'chord': None}, **body)




tid = uuid()
hostname = socket.gethostname()
task = {'task': T.name, 'args': (), 'kwargs': {}, 'id': tid, 'flags': 0}
app = current_app._get_current_object()

m = Message(None, {}, {}, task)

ts = time()
for i in xrange(1000000):
    x = Request(task, hostname=hostname, app=app, task=task)
print(time() - ts)

