"""Task Routing.

Contains utilities for working with task routers, (:setting:`task_routes`).
"""
import fnmatch
import re
import warnings
from collections.abc import Mapping

from kombu import Queue

from celery.exceptions import QueueNotFound, CPendingDeprecationWarning
from celery.utils.collections import lpmerge
from celery.utils.functional import maybe_evaluate, mlazy
from celery.utils.imports import symbol_by_name

try:
    Pattern = re._pattern_type
except AttributeError:  # pragma: no cover
    # for support Python 3.7
    Pattern = re.Pattern

__all__ = ('MapRoute', 'Router', 'expand_router_string', 'prepare')

GLOB_PATTERNS = ('*', '?', '[', ']', '!')

MAP_ROUTES_MUST_BE_A_DICTIONARY = (
    "Starting from Celery 5.1 the task_routes configuration must be a dictionary. "
    "Support for providing a list of router objects will be removed in 6.0."
)


class MapRoute:
    """Creates a router out of a :class:`dict`."""

    def __init__(self, map):
        # map is either a mapping or a an iterable of tuples
        if isinstance(map, Mapping):
            map = map.items()
        else:
            warnings.warn(
                CPendingDeprecationWarning(MAP_ROUTES_MUST_BE_A_DICTIONARY))
        self.map = {}
        patterns = {}
        for k, v in map:
            if isinstance(k, Pattern):
                # This is already a regular expression so we simply store it.
                patterns[k] = v
            elif any(glob_pattern in k for glob_pattern in GLOB_PATTERNS):
                # This is a glob pattern so we
                # must to translate it into a regular expression.
                patterns[re.compile(fnmatch.translate(k))] = v
            else:
                # This is a direct mapping between a task and a routing
                # so we simply store it.
                self.map[k] = v

        # We sort by the regex pattern's length since longer regex patterns
        # are likely to be more specific.
        self.patterns = tuple(reversed(sorted(patterns.items(),
                                              key=lambda item: len(item[0].pattern))))

    def __call__(self, name, *args, **kwargs):
        try:
            return dict(self.map[name])
        except KeyError:
            pass
        except ValueError:
            # if self.map[name] is a string we consider it to be the queue's
            # name.
            return {'queue': self.map[name]}
        for regex, route in self.patterns:
            if regex.match(name):
                try:
                    return dict(route)
                except ValueError:
                    # if route is a string we consider it to be the queue's
                    # name.
                    return {'queue': route}


class Router:
    """Route tasks based on the :setting:`task_routes` setting."""

    def __init__(self, routes=None, queues=None,
                 create_missing=False, app=None):
        self.app = app
        self.queues = {} if queues is None else queues
        self.routes = [] if routes is None else routes
        self.create_missing = create_missing

    def route(self, options, name, args=(), kwargs=None, task_type=None):
        kwargs = {} if not kwargs else kwargs
        options = self.expand_destination(options)  # expands 'queue'
        if self.routes:
            route = self.lookup_route(name, args, kwargs, options, task_type)
            if route:  # expands 'queue' in route.
                return lpmerge(self.expand_destination(route), options)
        if 'queue' not in options:
            options = lpmerge(self.expand_destination(
                self.app.conf.task_default_queue), options)
        return options

    def expand_destination(self, route):
        # Route can be a queue name: convenient for direct exchanges.
        if isinstance(route, str):
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
                        f'Queue {queue!r} missing from task_queues')
        return route

    def lookup_route(self, name,
                     args=None, kwargs=None, options=None, task_type=None):
        query = self.query_router
        for router in self.routes:
            route = query(router, name, args, kwargs, options, task_type)
            if route is not None:
                return route

    def query_router(self, router, task, args, kwargs, options, task_type):
        router = maybe_evaluate(router)
        if hasattr(router, 'route_for_task'):
            # pre 4.0 router class
            return router.route_for_task(task, args, kwargs)
        return router(task, args, kwargs, options, task=task_type)


def expand_router_string(router):
    router = symbol_by_name(router)
    if hasattr(router, 'route_for_task'):
        # need to instantiate pre 4.0 router classes
        router = router()
    return router


def prepare(routes):
    """Expand the :setting:`task_routes` setting."""

    def expand_route(route):
        if isinstance(route, (Mapping, list, tuple)):
            return MapRoute(route)
        if isinstance(route, str):
            return mlazy(expand_router_string, route)
        return route

    if routes is None:
        return ()
    if not isinstance(routes, (list, tuple)):
        routes = (routes,)
    return [expand_route(route) for route in routes]
