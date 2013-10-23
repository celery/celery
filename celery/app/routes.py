# -*- coding: utf-8 -*-
"""
    celery.routes
    ~~~~~~~~~~~~~

    Contains utilities for working with task routers,
    (:setting:`CELERY_ROUTES`).

"""
from __future__ import absolute_import

from celery.exceptions import QueueNotFound
from celery.five import string_t
from celery.utils import lpmerge
from celery.utils.functional import firstmethod, mlazy
from celery.utils.imports import instantiate

__all__ = ['MapRoute', 'Router', 'prepare']

_first_route = firstmethod('route_for_task')


class MapRoute(object):
    """Creates a router out of a :class:`dict`."""

    no_route = object()  # sentinel value representing task w/ no route

    def __init__(self, map):
        self.map = {}
        self.prefix_map = {}

        for name, route in map.items():  # discern routes that are prefix vs. fully-qualified
            if name.endswith('.'):
                self.prefix_map[name[:-1]] = route  # slice off period suffix
            else:
                self.map[name] = route

    def route_for_task(self, task, *args, **kwargs):

        def derive_route(name):
            split = name.rsplit('.', 1)
            if len(split) == 1:  # can't traverse any deeper
                self.map[task] = self.no_route
                return

            name = split[0]
            route = self.prefix_map.get(name)
            if route:
                self.map[task] = route
                return dict(route)
            return derive_route(name)

        route = self.map.get(task)
        if route == self.no_route:
            return
        if route:
            return dict(route)
        return derive_route(task)


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
            route = self.lookup_route(task, args, kwargs)
            if route:  # expands 'queue' in route.
                return lpmerge(self.expand_destination(route), options)
        if 'queue' not in options:
            options = lpmerge(self.expand_destination(
                              self.app.conf.CELERY_DEFAULT_QUEUE), options)
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
            try:
                Q = self.queues[queue]  # noqa
            except KeyError:
                raise QueueNotFound(
                    'Queue {0!r} missing from CELERY_QUEUES'.format(queue))
            # needs to be declared by publisher
            route['queue'] = Q
        return route

    def lookup_route(self, task, args=None, kwargs=None):
        return _first_route(self.routes, task, args, kwargs)


def prepare(routes):
    """Expands the :setting:`CELERY_ROUTES` setting."""

    def expand_route(route):
        if isinstance(route, dict):
            return MapRoute(route)
        if isinstance(route, string_t):
            return mlazy(instantiate, route)
        return route

    if routes is None:
        return ()
    if not isinstance(routes, (list, tuple)):
        routes = (routes, )
    return [expand_route(route) for route in routes]
