# -*- coding: utf-8 -*-
"""
    celery.routes
    ~~~~~~~~~~~~~

    Contains utilities for working with task routers,
    (:setting:`task_routes`).

"""
from __future__ import absolute_import, unicode_literals

import re
import string

from collections import Mapping, OrderedDict

from kombu import Queue

from celery.exceptions import QueueNotFound
from celery.five import items, string_t
from celery.utils import lpmerge
from celery.utils.functional import firstmethod, fun_takes_argument, mlazy
from celery.utils.imports import instantiate

__all__ = ['MapRoute', 'Router', 'prepare']


def _try_route(meth, task, args, kwargs, options=None):
    if fun_takes_argument('options', meth, position=4):
        return meth(task, args, kwargs, options)
    return meth(task, args, kwargs)

_first_route = firstmethod('route_for_task')


def glob_to_re(glob, quote=string.punctuation.replace('*', '')):
    glob = ''.join('\\' + c if c in quote else c for c in glob)
    return glob.replace('*', '.+?')


class MapRoute(object):
    """Creates a router out of a :class:`dict`."""

    def __init__(self, map):
        map = items(map) if isinstance(map, Mapping) else map
        self.map = {}
        self.patterns = OrderedDict()
        for k, v in map:
            if isinstance(k, re._pattern_type):
                self.patterns[k] = v
            elif '*' in k:
                self.patterns[re.compile(glob_to_re(k))] = v
            else:
                self.map[k] = v

    def route_for_task(self, task, *args, **kwargs):
        try:
            return dict(self.map[task])
        except KeyError:
            pass
        except ValueError:
            return {'queue': self.map[task]}
        for regex, route in items(self.patterns):
            if regex.match(task):
                try:
                    return dict(route)
                except ValueError:
                    return {'queue': route}


class Router(object):

    def __init__(self, routes=None, queues=None,
                 create_missing=False, app=None):
        self.app = app
        self.queues = {} if queues is None else queues
        self.routes = [] if routes is None else routes
        self.create_missing = create_missing

    def route(self, options, task, args=(), kwargs={}):
        options = self.expand_destination(options)  # expands 'queue'
        if self.routes:
            route = self.lookup_route(task, args, kwargs, options)
            if route:  # expands 'queue' in route.
                return lpmerge(self.expand_destination(route), options)
        if 'queue' not in options:
            options = lpmerge(self.expand_destination(
                              self.app.conf.task_default_queue), options)
        return options

    def expand_destination(self, route):
        # Route can be a queue name: convenient for direct exchanges.
        if isinstance(route, string_t):
            queue, route = route, {}
        else:
            # can use defaults from configured queue, but override specific
            # things (like the routing_key): great for topic exchanges.
            queue = route.pop('queue', None)

        if queue:
            if isinstance(queue, Queue):
                route['queue'] = queue
            else:
                try:
                    route['queue'] = self.queues[queue]
                except KeyError:
                    raise QueueNotFound(
                        'Queue {0!r} missing from task_queues'.format(queue))
        return route

    def lookup_route(self, task, args=None, kwargs=None, options=None):
        return _first_route(self.routes, task, args, kwargs, options)


def prepare(routes):
    """Expands the :setting:`task_routes` setting."""

    def expand_route(route):
        if isinstance(route, (Mapping, list, tuple)):
            return MapRoute(route)
        if isinstance(route, string_t):
            return mlazy(instantiate, route)
        return route

    if routes is None:
        return ()
    if not isinstance(routes, (list, tuple)):
        routes = (routes,)
    return [expand_route(route) for route in routes]
