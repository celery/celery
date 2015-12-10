from __future__ import absolute_import

import os

from celery.five import items
from kombu import Exchange, Queue
from kombu.utils import symbol_by_name

CSTRESS_TRANS = os.environ.get('CSTRESS_TRANS', False)
default_queue = 'c.stress.trans' if CSTRESS_TRANS else 'c.stress'
CSTRESS_QUEUE = os.environ.get('CSTRESS_QUEUE_NAME', default_queue)

templates = {}


def template(name=None):

    def _register(cls):
        templates[name or cls.__name__] = '.'.join([__name__, cls.__name__])
        return cls
    return _register


def use_template(app, template='default'):
    template = template.split(',')

    # mixin the rest of the templates when the config is needed
    @app.on_after_configure.connect(weak=False)
    def load_template(sender, source, **kwargs):
        mixin_templates(template[1:], source)

    app.config_from_object(templates[template[0]])


def mixin_templates(templates, conf):
    return [mixin_template(template, conf) for template in templates]


def mixin_template(template, conf):
    cls = symbol_by_name(templates[template])
    conf.update(dict(
        (k, v) for k, v in items(vars(cls))
        if not k.startswith('_')
    ))


def template_names():
    return ', '.join(templates)


@template()
class default(object):
    accept_content = ['json']
    broker_url = os.environ.get('CSTRESS_BROKER', 'pyamqp://')
    broker_heartbeat = 30
    result_backend = os.environ.get('CSTRESS_BACKEND', 'rpc://')
    result_serializer = 'json'
    result_persistent = True
    result_expires = 300
    result_cache_max = 100
    task_default_queue = CSTRESS_QUEUE
    task_queues = [
        Queue(CSTRESS_QUEUE,
              durable=not CSTRESS_TRANS,
              no_ack=CSTRESS_TRANS),
    ]
    task_serializer = 'json'
    task_publish_retry_policy = {
        'max_retries': 100,
        'interval_max': 2,
        'interval_step': 0.1,
    }
    task_protocol = 2
    if CSTRESS_TRANS:
        task_default_delivery_mode = 1
    worker_prefetch_multiplier = int(os.environ.get('CSTRESS_PREFETCH', 10))


@template()
class redis(default):
    broker_url = os.environ.get('CSTRESS_BROKER', 'redis://')
    broker_transport_options = {
        'fanout_prefix': True,
        'fanout_patterns': True,
    }
    result_backend = os.environ.get('CSTRESS_BACKEND', 'redis://')


@template()
class redistore(default):
    result_backend = 'redis://'


@template()
class acks_late(default):
    task_acks_late = True


@template()
class pickle(default):
    accept_content = ['pickle', 'json']
    task_serializer = 'pickle'
    result_serializer = 'pickle'


@template()
class confirms(default):
    broker_url = 'pyamqp://'
    broker_transport_options = {'confirm_publish': True}


@template()
class events(default):
    task_send_events = True
    task_send_sent_event = True


@template()
class execv(default):
    worker_force_execv = True


@template()
class sqs(default):
    broker_url = 'sqs://'
    broker_transport_options = {
        'region': os.environ.get('AWS_REGION', 'us-east-1'),
    }


@template()
class proto1(default):
    task_protocol = 1
