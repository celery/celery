import unittest2 as unittest


from celery import conf
from celery import routes
from celery.utils.functional import wraps
from celery.exceptions import RouteNotFound


def E(routing_table):
    def expand(answer):
        return routes.expand_destination(answer, routing_table)
    return expand


def with_queues(**queues):

    def patch_fun(fun):
        @wraps(fun)
        def __inner(*args, **kwargs):
            prev_queues = conf.QUEUES
            conf.QUEUES = queues
            try:
                return fun(*args, **kwargs)
            finally:
                conf.QUEUES = prev_queues
        return __inner
    return patch_fun


a_route = {"exchange": "fooexchange",
           "exchange_type": "fanout",
               "binding_key": "xuzzy"}
b_route = {"exchange": "barexchange",
           "exchange_type": "topic",
           "binding_key": "b.b.#"}


class test_MapRoute(unittest.TestCase):

    @with_queues(foo=a_route, bar=b_route)
    def test_route_for_task_expanded_route(self):
        expand = E(conf.QUEUES)
        route = routes.MapRoute({"celery.ping": "foo"})
        self.assertDictContainsSubset(a_route,
                             expand(route.route_for_task("celery.ping")))
        self.assertIsNone(route.route_for_task("celery.awesome"))

    @with_queues(foo=a_route, bar=b_route)
    def test_route_for_task(self):
        expand = E(conf.QUEUES)
        route = routes.MapRoute({"celery.ping": b_route})
        self.assertDictContainsSubset(b_route,
                             expand(route.route_for_task("celery.ping")))
        self.assertIsNone(route.route_for_task("celery.awesome"))

    def test_expand_route_not_found(self):
        expand = E(conf.QUEUES)
        route = routes.MapRoute({"a": "x"})
        self.assertRaises(RouteNotFound, expand, route.route_for_task("a"))


class test_lookup_route(unittest.TestCase):

    @with_queues(foo=a_route, bar=b_route)
    def test_lookup_takes_first(self):
        expand = E(conf.QUEUES)
        R = routes.prepare(({"celery.ping": "bar"},
                            {"celery.ping": "foo"}))
        self.assertDictContainsSubset(b_route,
                expand(routes.lookup_route(R, "celery.ping",
                    args=[1, 2], kwargs={})))

    @with_queues(foo=a_route, bar=b_route)
    def test_lookup_paths_traversed(self):
        expand = E(conf.QUEUES)
        R = routes.prepare(({"celery.xaza": "bar"},
                            {"celery.ping": "foo"}))
        self.assertDictContainsSubset(a_route,
                expand(routes.lookup_route(R, "celery.ping",
                    args=[1, 2], kwargs={})))
        self.assertIsNone(routes.lookup_route(R, "celery.poza"))


class test_lookup_disabled(unittest.TestCase):

    def test_disabled(self):

        def create_router(name, is_disabled):
            class _Router(object):

                def disabled(self, task, *args):
                    if task == name:
                        return is_disabled
            return _Router()


        A = create_router("celery.ping", True)
        B = create_router("celery.ping", False)
        C = object()

        R1 = (routes.prepare((A, B, C)), True)
        R2 = (routes.prepare((B, C, A)), False)
        R3 = (routes.prepare((C, A, B)), True)
        R4 = (routes.prepare((B, A, C)), False)
        R5 = (routes.prepare((A, C, B)), True)
        R6 = (routes.prepare((C, B, A)), False)

        for i, (router, state) in enumerate((R1, R2, R3, R4, R5, R6)):
            self.assertEqual(routes.lookup_disabled(router, "celery.ping"),
                             state, "ok %d" % i)
