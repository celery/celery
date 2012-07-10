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

    def __repr__(self):
        return '%s/%s' % (self.count, self.strtotal)


def republish(producer, message, exchange=None, routing_key=None,
        remove_props=['application_headers',
                      'content_type',
                      'content_encoding',
                      'headers']):
    body = ensure_bytes(message.body)  # use raw message body.
    info, headers, props = (message.delivery_info,
                            message.headers, message.properties)
    exchange = info['exchange'] if exchange is None else exchange
    routing_key = info['routing_key'] if routing_key is None else routing_key
    ctype, enc = message.content_type, message.content_encoding
    # remove compression header, as this will be inserted again
    # when the message is recompressed.
    compression = headers.pop('compression', None)

    for key in remove_props:
        props.pop(key, None)

    producer.publish(ensure_bytes(body), exchange=exchange,
                     routing_key=routing_key, compression=compression,
                     headers=headers, content_type=ctype,
                     content_encoding=enc, **props)


def migrate_task(producer, body_, message, queues=None):
    info = message.delivery_info
    queues = {} if queues is None else queues
    republish(producer, message,
              exchange=queues.get(info['exchange']),
              routing_key=queues.get(info['routing_key']))


def filter_callback(callback, tasks):

    def filtered(body, message):
        if tasks and message.payload['task'] not in tasks:
            return

        return callback(body, message)
    return filtered


def migrate_tasks(source, dest, migrate=migrate_task, app=None,
        queues=None, **kwargs):
    app = app_or_default(app)
    queues = prepare_queues(queues)
    producer = app.amqp.TaskProducer(dest)
    migrate = partial(migrate, producer, queues=queues)

    def on_declare_queue(queue):
        new_queue = queue(producer.channel)
        new_queue.name = queues.get(queue.name, queue.name)
        if new_queue.routing_key == queue.name:
            new_queue.routing_key = queues.get(queue.name,
                                               new_queue.routing_key)
        if new_queue.exchange.name == queue.name:
            new_queue.exchange.name = queues.get(queue.name, queue.name)
        new_queue.declare()

    return start_filter(app, source, migrate, queues=queues,
                        on_declare_queue=on_declare_queue, **kwargs)


def move_tasks(conn, predicate, exchange, routing_key, app=None, **kwargs):
    """Find tasks by filtering them and move the tasks to a new queue.

    :param conn: Connection to use.
    :param predicate: Filter function with signature ``(body, message)``.
    :param exchange: Destination exchange.
    :param routing_key: Destination routing key.

    Also supports the same keyword arguments as :func:`start_filter`.

    To demonstrate, the :func:`move_task_by_id` operation can be implemented
    like this:

    .. code-block:: python

        def is_wanted_task(body, message):
            if body['id'] == wanted_id:
                return True

        move_tasks(conn, is_wanted_task, exchange, routing_key)

    """
    app = app_or_default(app)
    producer = app.amqp.TaskProducer(conn)

    def on_task(body, message):
        if predicate(body, message):
            republish(producer, message,
                      exchange=exchange, routing_key=routing_key)
            message.ack()

    return start_filter(app, conn, on_task, **kwargs)


def move_task_by_id(conn, task_id, exchange, routing_key, **kwargs):
    """Find a task by id and move it to another queue.

    :param conn: Connection to use.
    :param task_id: Id of task to move.
    :param exchange: Destination exchange.
    :param exchange: Destination routing key.

    Also supports the same keyword arguments as :func:`start_filter`.

    """
    def predicate(body, message):
        if body['id'] == task_id:
            return True

    return move_tasks(conn, predicate, exchange, routing_key, **kwargs)


def prepare_queues(queues):
    if isinstance(queues, basestring):
        queues = queues.split(',')
    if isinstance(queues, list):
        queues = dict(tuple(islice(cycle(q.split(':')), None, 2))
                        for q in queues)
    if queues is None:
        queues = {}
    return queues


def start_filter(app, conn, filter, limit=None, timeout=1.0,
        ack_messages=False, migrate=migrate_task, tasks=None, queues=None,
        callback=None, forever=False, on_declare_queue=None, **kwargs):
    state = State()
    queues = prepare_queues(queues)
    if isinstance(tasks, basestring):
        tasks = set(tasks.split(','))
    if tasks is None:
        tasks = set([])

    def update_state(body, message):
        state.count += 1

    def ack_message(body, message):
        message.ack()

    consumer = app.amqp.TaskConsumer(conn)

    if tasks:
        filter = filter_callback(filter, tasks)
        update_state = filter_callback(update_state, tasks)
        ack_message = filter_callback(ack_message, tasks)

    consumer.register_callback(filter)
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
        if on_declare_queue is not None:
            on_declare_queue(queue)
        try:
            _, mcount, _ = queue(consumer.channel).queue_declare(passive=True)
            if mcount:
                state.total_apx += mcount
        except conn.channel_errors + (StdChannelError, ):
            pass

    # start migrating messages.
    with consumer:
        try:
            for _ in eventloop(conn, limit=limit,  # pragma: no cover
                               timeout=timeout, ignore_timeouts=forever):
                pass
        except socket.timeout:
            pass
    return state
