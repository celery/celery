# -*- coding: utf-8 -*-
from __future__ import absolute_import

import warnings

from celery.utils.text import pluralize
from celery.exceptions import DuplicateNodenameWarning

__all__ = ['Inspect', 'flatten_reply']


W_DUPNODE = """\
Received multiple replies from node name: {0!r}.
Please make sure you give each node a unique nodename using the `-n` option.\
"""


def flatten_reply(reply):
    nodes, dupes = {}, set()
    for item in reply:
        [dupes.add(name) for name in item if name in nodes]
        nodes.update(item)
    if dupes:
        warnings.warn(DuplicateNodenameWarning(
            W_DUPNODE.format(
                pluralize(len(dupes), 'name'), ', '.join(sorted(dupes)),
            ),
        ))
    return nodes


class Inspect(object):
    app = None

    def __init__(self, destination=None, timeout=1, callback=None,
                 connection=None, app=None, limit=None):
        self.app = app or self.app
        self.destination = destination
        self.timeout = timeout
        self.callback = callback
        self.connection = connection
        self.limit = limit

    def _prepare(self, reply):
        if not reply:
            return
        by_node = flatten_reply(reply)
        if self.destination and \
                not isinstance(self.destination, (list, tuple)):
            return by_node.get(self.destination)
        return by_node

    def _request(self, command, **kwargs):
        return self._prepare(self.app.control.broadcast(
            command,
            arguments=kwargs,
            destination=self.destination,
            callback=self.callback,
            connection=self.connection,
            limit=self.limit,
            timeout=self.timeout, reply=True,
        ))

    def report(self):
        return self._request('report')

    def clock(self):
        return self._request('clock')

    def active(self, safe=False):
        return self._request('dump_active', safe=safe)

    def scheduled(self, safe=False):
        return self._request('dump_schedule', safe=safe)

    def reserved(self, safe=False):
        return self._request('dump_reserved', safe=safe)

    def stats(self):
        return self._request('stats')

    def revoked(self):
        return self._request('dump_revoked')

    def registered(self, *taskinfoitems):
        return self._request('dump_tasks', taskinfoitems=taskinfoitems)
    registered_tasks = registered

    def ping(self):
        return self._request('ping')

    def active_queues(self):
        return self._request('active_queues')

    def query_task(self, ids):
        return self._request('query_task', ids=ids)

    def conf(self, with_defaults=False):
        return self._request('dump_conf', with_defaults=with_defaults)

    def hello(self, from_node, revoked=None):
        return self._request('hello', from_node=from_node, revoked=revoked)

    def memsample(self):
        return self._request('memsample')

    def memdump(self, samples=10):
        return self._request('memdump', samples=samples)

    def objgraph(self, type='Request', n=200, max_depth=10):
        return self._request('objgraph', num=n, max_depth=max_depth, type=type)
