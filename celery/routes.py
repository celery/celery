from celery.exceptions import QueueNotFound
from celery.utils import instantiate, firstmethod, mpromise

_first_route = firstmethod("route_for_task")


class MapRoute(object):
    """Makes a router out of a :class:`dict`."""

    def __init__(self, map):
        self.map = map

    def route_for_task(self, task, *args, **kwargs):
        return self.map.get(task)


class Router(object):

    def __init__(self, routes=None, queues=None, create_missing=False):
        if queues is None:
            queues = {}
        if routes is None:
            routes = []
        self.queues = queues
        self.routes = routes
        self.create_missing = create_missing

    def add_queue(self, queue):
        q = self.queues[queue] = {"binding_key": queue,
                                  "routing_key": queue,
                                  "exchange": queue,
                                  "exchange_type": "direct"}
        return q

    def route(self, options, task, args=(), kwargs={}):
        # Expand "queue" keys in options.
        options = self.expand_destination(options)
        if self.routes:
            route = self.lookup_route(task, args, kwargs)
            if route:
                # Also expand "queue" keys in route.
                return dict(options, **self.expand_destination(route))
        return options

    def expand_destination(self, route):
        # The route can simply be a queue name,
        # this is convenient for direct exchanges.
        if isinstance(route, basestring):
            queue, route = route, {}
        else:
            # For topic exchanges you can use the defaults from a queue
            # definition, and override e.g. just the routing_key.
            queue = route.pop("queue", None)

        if queue:
            try:
                dest = dict(self.queues[queue])
            except KeyError:
                if self.create_missing:
                    dest = self.add_queue(queue)
                else:
                    raise QueueNotFound(
                        "Queue '%s' is not defined in CELERY_QUEUES" % queue)
            dest.setdefault("routing_key", dest.get("binding_key"))
            return dict(route, **dest)

        return route

    def lookup_route(self, task, args=None, kwargs=None):
        return _first_route(self.routes, task, args, kwargs)


def prepare(routes):
    """Expand ROUTES setting."""

    def expand_route(route):
        if isinstance(route, dict):
            return MapRoute(route)
        if isinstance(route, basestring):
            return mpromise(instantiate, route)
        return route

    if not isinstance(routes, (list, tuple)):
        routes = (routes, )
    return map(expand_route, routes)
