# -*- coding: utf-8 -*-
"""This module is deprecated, use ``current_app.amqp`` instead."""
from __future__ import absolute_import

from . import current_app
from .local import Proxy

TaskPublisher = Proxy(lambda: current_app.amqp.TaskPublisher)
ConsumerSet = Proxy(lambda: current_app.amqp.ConsumerSet)
TaskConsumer = Proxy(lambda: current_app.amqp.TaskConsumer)
establish_connection = Proxy(lambda: current_app.broker_connection)
with_connection = Proxy(lambda: current_app.with_default_connection)
get_consumer_set = Proxy(lambda: current_app.amqp.get_task_consumer)
