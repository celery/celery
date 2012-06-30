# -*- coding: utf-8 -*-
"""
    celery.contrib.migrate
    ~~~~~~~~~~~~~~~~~~~~~~

    Migration tools.

"""
from __future__ import absolute_import
from __future__ import with_statement

import socket

from functools import partial
from itertools import cycle, islice

from kombu import eventloop
from kombu.exceptions import StdChannelError
from kombu.utils.encoding import ensure_bytes

from celery.app import app_or_default


class State(object):
    count = 0
    total_apx = 0

    @property
    def strtotal(self):
        if not self.total_apx:
            return u'?'
        return unicode(self.total_apx)


def migrate_task(producer, body_, message, queues=None,
        remove_props=['application_headers',
                      'content_type',
                      'content_encoding',
                      'headers']):
    queues = {} if queues is None else queues
    body = ensure_bytes(message.body)  # use raw message body.
    info, headers, props = (message.delivery_info,
                            message.headers,
                            message.properties)
    ctype, enc = message.content_type, message.content_encoding
    # remove compression header, as this will be inserted again
    # when the message is recompressed.
    compression = headers.pop('compression', None)

    for key in remove_props:
        props.pop(key, None)

    exchange = queues.get(info['exchange'], info['exchange'])
    routing_key = queues.get(info['routing_key'], info['routing_key'])

    producer.publish(ensure_bytes(body), exchange=exchange,
                           routing_key=routing_key,
                           compression=compression,
                           headers=headers,
                           content_type=ctype,
                           content_encoding=enc,
                           **props)


def filter_callback(callback, tasks):
    def filtered(body, message):
        if tasks and message.payload['task'] not in tasks:
            return

        return callback(body, message)
    return filtered


def migrate_tasks(source, dest, limit=None, timeout=1.0, ack_messages=False,
        app=None, migrate=migrate_task, tasks=None, queues=None, callback=None,
        forever=False, **kwargs):
    state = State()
    app = app_or_default(app)

    if isinstance(queues, basestring):
        queues = queues.split(',')
    if isinstance(queues, list):
        queues = dict(tuple(islice(cycle(q.split(':')), None, 2))
                        for q in queues)
    if queues is None:
        queues = {}

    if isinstance(tasks, basestring):
        tasks = set(tasks.split(','))
    if tasks is None:
        tasks = set([])

    def update_state(body, message):
        state.count += 1

    def ack_message(body, message):
        message.ack()

    producer = app.amqp.TaskProducer(dest)
    migrate = partial(migrate, producer, queues=queues)
    consumer = app.amqp.TaskConsumer(source)

    if tasks:
        migrate = filter_callback(migrate, tasks)
        update_state = filter_callback(update_state, tasks)
        ack_message = filter_callback(ack_message, tasks)

    consumer.register_callback(migrate)
    consumer.register_callback(update_state)
    if ack_messages:
        consumer.register_callback(ack_message)
    if callback is not None:
        callback = partial(callback, state)
        if tasks:
            callback = filter_callback(callback, tasks)
        consumer.register_callback(callback)

    # declare all queues on the new broker.
    for queue in consumer.queues:
        if queues and queue.name not in queues:
            continue

        new_queue = queue(producer.channel)
        new_queue.name = queues.get(queue.name, queue.name)
        if new_queue.routing_key == queue.name:
            new_queue.routing_key = queues.get(queue.name,
                                               new_queue.routing_key)
        if new_queue.exchange.name == queue.name:
            new_queue.exchange.name = queues.get(queue.name, queue.name)
        new_queue.declare()

        try:
            _, mcount, _ = queue(consumer.channel).queue_declare(passive=True)
            if mcount:
                state.total_apx += mcount
        except source.channel_errors + (StdChannelError, ):
            pass

    # start migrating messages.
    with consumer:
        try:
            for _ in eventloop(source, limit=limit,  # pragma: no cover
                               timeout=timeout, ignore_timeouts=forever):
                pass
        except socket.timeout:
            return
