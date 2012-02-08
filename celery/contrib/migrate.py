# -*- coding: utf-8 -*-
"""
    celery.contrib.migrate
    ~~~~~~~~~~~~~~~~~~~~~~

    Migration tools.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import socket

from functools import partial

from kombu.common import eventloop
from kombu.exceptions import StdChannelError
from kombu.utils.encoding import ensure_bytes

from celery.app import app_or_default


class State(object):
    count = 0
    total_apx = 0

    @property
    def strtotal(self):
        if not self.total_apx:
            return u"?"
        return unicode(self.total_apx)


def migrate_task(producer, body_, message,
        remove_props=["application_headers",
                      "content_type",
                      "content_encoding"]):
    body = ensure_bytes(message.body)  # use raw message body.
    info, headers, props = (message.delivery_info,
                            message.headers,
                            message.properties)
    ctype, enc = message.content_type, message.content_encoding
    # remove compression header, as this will be inserted again
    # when the message is recompressed.
    compression = headers.pop("compression", None)

    for key in remove_props:
        props.pop(key, None)

    producer.publish(ensure_bytes(body), exchange=info["exchange"],
                           routing_key=info["routing_key"],
                           compression=compression,
                           headers=headers,
                           content_type=ctype,
                           content_encoding=enc,
                           **props)


def migrate_tasks(source, dest, timeout=1.0, app=None,
        migrate=None, callback=None):
    state = State()
    app = app_or_default(app)

    def update_state(body, message):
        state.count += 1

    producer = app.amqp.TaskPublisher(dest)
    if migrate is None:
        migrate = partial(migrate_task, producer)
    if callback is not None:
        callback = partial(callback, state)
    consumer = app.amqp.get_task_consumer(source)
    consumer.register_callback(update_state)
    consumer.register_callback(callback)
    consumer.register_callback(migrate)

    # declare all queues on the new broker.
    for queue in consumer.queues:
        queue(producer.channel).declare()
        try:
            _, mcount, _ = queue(consumer.channel).queue_declare(passive=True)
            if mcount:
                state.total_apx += mcount
        except source.channel_errors + (StdChannelError, ):
            pass

    # start migrating messages.
    with consumer:
        try:
            for _ in eventloop(source, timeout=timeout):
                pass
        except socket.timeout:
            return
