from celery.exceptions import RouteNotFound
from celery.utils import instantiate


class MapRoute(object):
    """Makes a router out of a :class:`dict`."""

    def __init__(self, map):
        self.map = map

    def route_for_task(self, task, *args, **kwargs):
        return self.map.get(task)


def expand_destination(route, routing_table):
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
            dest = dict(routing_table[queue])
        except KeyError:
            raise RouteNotFound(
                "Route %s does not exist in the routing table "
                "(CELERY_QUEUES)" % route)
        dest.setdefault("routing_key", dest.get("binding_key"))
        return dict(route, **dest)

    return route


def prepare(routes):
    """Expand ROUTES setting."""

    def expand_route(route):
        if isinstance(route, dict):
            return MapRoute(route)
        if isinstance(route, basestring):
            return instantiate(route)
        return route

    if not hasattr(routes, "__iter__"):
        routes = (routes, )
    return map(expand_route, routes)


def route(routes, options, routing_table, task, args=(), kwargs={}):
    # Expand "queue" keys in options.
    options = expand_destination(options, routing_table)
    if routes:
        route = lookup_route(routes, task, args, kwargs)
        # Also expand "queue" keys in route.
        return dict(options, **expand_destination(route, routing_table))
    return options


def firstmatcher(method):
    """Returns a functions that with a list of instances,
    finds the first instance that returns a value for the given method."""

    def _matcher(seq, *args, **kwargs):
        for cls in seq:
            try:
                answer = getattr(cls, method)(*args, **kwargs)
                if answer is not None:
                    return answer
            except AttributeError:
                pass
    return _matcher


_first_route = firstmatcher("route_for_task")
_first_disabled = firstmatcher("disabled")


def lookup_route(routes, task, args=None, kwargs=None):
    return _first_route(routes, task, args, kwargs)


def lookup_disabled(routes, task, args=None, kwargs=None):
    return _first_disabled(routes, task, args, kwargs)
