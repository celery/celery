"""Celery worker app module declaring a queue selected by its alias.

Used as the app module of a worker container to declare a queue topology
with :class:`kombu.Queue` objects, which cannot be serialized through the
worker configuration changes.
"""

from __future__ import annotations

import json

from kombu import Queue

from celery import Celery

imports = None

app = Celery("celery_test_app")
config = None

if config:
    app.config_from_object(config)
    print(f"Changed worker configuration: {json.dumps(config, indent=4)}")

app.conf.task_queues = [Queue("real_name", alias="alias")]

if __name__ == "__main__":
    app.start()
