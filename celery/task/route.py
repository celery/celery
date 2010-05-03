from celery import conf
from celery.utils import get_cls_by_name

default_queue = conf.routing_table[conf.DEFAULT_QUEUE]


# Custom Router
class Router(object):

    def route_for_task(self, task, task_id=None, args=None, kwargs=None):
        return {"queue": conf.DEFAULT_QUEUE,
                "exchange": default_queue["exchange"],
                "routing_key": conf.DEFAULT_ROUTING_KEY}

    def disabled(self, task, task_id=None, args=None, kwargs=None):
        if task.name == "celery.ping":
            return True



# Route from mapping
class MapRoute(object):

    def __init__(self, map):
        self.map = dict((name, self._expand_destination(entry)
                            for name, entry in map.items())

    def route_for_task(self, task, **kwargs):
        return self.map.get(task.name)

    def _expand_destination(self, entry):
        if isinstance(entry, basestring):
            dest = dict(conf.routing_table[entry])
            dest.setdefault("routing_key", dest.get("binding_key"))
            return dest
        return entry



# CELERY_ROUTES initialization
"""

    >>> CELERY_ROUTES = {"celery.ping": "default",
                          "mytasks.add": "cpu-bound",
                          "video.encode": {
                            "queue": "video",
                            "exchange": "media"
                            "routing_key": "media.video.encode"}}

    >>> CELERY_ROUTES = ("myapp.tasks.Router",
                         {"celery.ping": "default})

"""


def expand_route(route):
    if hasattr(route, "items"):
        return MapRoute(route)
    if isinstance(route, "basestring"):
        return get_cls_by_name(route)()
    return route

routes = _get("CELERY_ROUTES", [])
if not hasattr(routes, "__iter__"):
    routes = (routes, )
routes = map(expand_route, routes)


# Traversing routes

def firstmatcher(seq, method):
    """With a list of instances, find the first instance that returns a
    value for the given method."""

    def _matcher(*args, **kwargs):
        for cls in seq:
            try:
                answer = getattr(cls, method)(*args, **kwargs)
                if answer:
                    return answer
            except AttributeError:
                pass

    return _matcher


_first_route = firstmatcher(routes, "route_for_task")
_first_disabled = firstmatcher(routes, "disabled")

def lookup_route(task, task_id=None, args=None, kwargs=None):
    return _first_route(task, task_id, args, kwargs)

def lookup_disabled(task, task_id=None, args=None, kwargs=None):
    return _first_disabled(task, task_id, args, kwargs)

