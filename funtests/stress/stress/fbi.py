from __future__ import absolute_import, print_function, unicode_literals

import socket
import sys

from contextlib import contextmanager

from celery import states


class FBI(object):

    def __init__(self, app):
        self.app = app
        self.receiver = None
        self.state = self.app.events.State()
        self.connection = None
        self.enabled = False

    def enable(self, enabled):
        self.enabled = enabled

    @contextmanager
    def investigation(self):
        if self.enabled:
            with self.app.connection() as conn:
                receiver = self.app.events.Receiver(
                    conn, handlers={'*': self.state.event},
                )
                with receiver.consumer_context() as (conn, _, _):
                    self.connection = conn
                    try:
                        yield self
                    finally:
                        self.ffwd()
        else:
            yield

    def ffwd(self):
        while 1:
            try:
                self.connection.drain_events(timeout=1)
            except socket.error:
                break

    def state_of(self, tid):
        try:
            task = self.state.tasks[tid]
        except KeyError:
            return 'No events for {0}'.format(tid)

        if task.state in states.READY_STATES:
            return 'Task {0.uuid} completed with {0.state}'.format(task)
        elif task.state in states.UNREADY_STATES:
            return 'Task {0.uuid} waiting in {0.state} state'.format(task)
        else:
            return 'Task {0.uuid} in other state {0.state}'.format(task)

    def query(self, ids):
        return self.app.control.inspect().query_task(id)

    def diag(self, ids, file=sys.stderr):
        if self.enabled:
            self.ffwd()
            for tid in ids:
                print(self.state_of(tid), file=file)
