from celery.exceptions import QueueNotFound
from celery.utils import firstmethod, instantiate, lpmerge, mpromise

_first_route = firstmethod("route_for_task")


class MapRoute(object):
    """Creates a router out of a :class:`dict`."""

    def __init__(self, map):
        self.map = map

    def route_for_task(self, task, *args, **kwargs):
        route = self.map.get(task)
        if route:
            return dict(route)


class Router(object):

    def __init__(self, routes=None, queues=None, create_missing=False,
            app=None):
        from celery.app import app_or_default
        self.app = app_or_default(app)
        if queues is None:
            queues = {}
        if routes is None:
            routes = []
        self.queues = queues
        self.routes = routes
        self.create_missing = create_missing

    def route(self, options, task, args=(), kwargs={}):
        options = self.expand_destination(options)  # expands 'queue'
        if self.routes:
            route = self.lookup_route(task, args, kwargs)
            if route:  # expands 'queue' in route.
                return lpmerge(self.expand_destination(route), options)
        if "queue" not in options:
            options = lpmerge(self.expand_destination(
                                self.app.conf.CELERY_DEFAULT_QUEUE), options)
        return options

    def expand_destination(self, route):
        # Route can be a queue name: convenient for direct exchanges.
        if isinstance(route, basestring):
            queue, route = route, {}
        else:
            # can use defaults from configured queue, but override specific
            # things (like the routing_key): great for topic exchanges.
            queue = route.pop("queue", None)

        if queue:  # expand config from configured queue.
            try:
                dest = dict(self.queues[queue])
            except KeyError:
                if not self.create_missing:
                    raise QueueNotFound(
                        "Queue %r is not defined in CELERY_QUEUES" % queue)
                dest = dict(self.app.amqp.queues.add(queue, queue, queue))
            # needs to be declared by publisher
            dest["queue"] = queue
            # routing_key and binding_key are synonyms.
            dest.setdefault("routing_key", dest.get("binding_key"))
            return lpmerge(dest, route)
        return route

    def lookup_route(self, task, args=None, kwargs=None):
        return _first_route(self.routes, task, args, kwargs)


def prepare(routes):
    """Expands the :setting:`CELERY_ROUTES` setting."""

    def expand_route(route):
        if isinstance(route, dict):
            return MapRoute(route)
        if isinstance(route, basestring):
            return mpromise(instantiate, route)
        return route

    if routes is None:
        return ()
    if not isinstance(routes, (list, tuple)):
        routes = (routes, )
    return map(expand_route, routes)
