# -*- coding: utf-8 -*-
"""
    celery.events.dumper
    ~~~~~~~~~~~~~~~~~~~~

    THis is a simple program that dumps events to the console
    as they happen.  Think of it like a `tcpdump` for Celery events.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys

from datetime import datetime

from ..app import app_or_default
from ..datastructures import LRUCache


TASK_NAMES = LRUCache(limit=0xFFF)

HUMAN_TYPES = {"worker-offline": "shutdown",
               "worker-online": "started",
               "worker-heartbeat": "heartbeat"}


def humanize_type(type):
    try:
        return HUMAN_TYPES[type.lower()]
    except KeyError:
        return type.lower().replace("-", " ")


class Dumper(object):

    def on_event(self, event):
        timestamp = datetime.fromtimestamp(event.pop("timestamp"))
        type = event.pop("type").lower()
        hostname = event.pop("hostname")
        if type.startswith("task-"):
            uuid = event.pop("uuid")
            if type in ("task-received", "task-sent"):
                task = TASK_NAMES[uuid] = "%s(%s) args=%s kwargs=%s" % (
                        event.pop("name"), uuid,
                        event.pop("args"),
                        event.pop("kwargs"))
            else:
                task = TASK_NAMES.get(uuid, "")
            return self.format_task_event(hostname, timestamp,
                                          type, task, event)
        fields = ", ".join("%s=%s" % (key, event[key])
                        for key in sorted(event.keys()))
        sep = fields and ":" or ""
        print("%s [%s] %s%s %s" % (hostname, timestamp,
                                    humanize_type(type), sep, fields))

    def format_task_event(self, hostname, timestamp, type, task, event):
        fields = ", ".join("%s=%s" % (key, event[key])
                        for key in sorted(event.keys()))
        sep = fields and ":" or ""
        print("%s [%s] %s%s %s %s" % (hostname, timestamp,
                                    humanize_type(type), sep, task, fields))


def evdump(app=None):
    sys.stderr.write("-> evdump: starting capture...\n")
    app = app_or_default(app)
    dumper = Dumper()
    conn = app.broker_connection()
    recv = app.events.Receiver(conn, handlers={"*": dumper.on_event})
    try:
        recv.capture()
    except (KeyboardInterrupt, SystemExit):
        conn and conn.close()

if __name__ == "__main__":
    evdump()
