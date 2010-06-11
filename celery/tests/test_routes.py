import unittest2 as unittest


from celery import conf
from celery import routes
from celery.utils.functional import wraps
from celery.exceptions import RouteNotFound


def E(queues):
    def expand(answer):
        return routes.Router([], queues).expand_destination(answer)
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


a_queue = {"exchange": "fooexchange",
           "exchange_type": "fanout",
               "binding_key": "xuzzy"}
b_queue = {"exchange": "barexchange",
           "exchange_type": "topic",
           "binding_key": "b.b.#"}


class test_MapRoute(unittest.TestCase):

    @with_queues(foo=a_queue, bar=b_queue)
    def test_route_for_task_expanded_route(self):
        expand = E(conf.QUEUES)
        route = routes.MapRoute({"celery.ping": "foo"})
        self.assertDictContainsSubset(a_queue,
                             expand(route.route_for_task("celery.ping")))
        self.assertIsNone(route.route_for_task("celery.awesome"))

    @with_queues(foo=a_queue, bar=b_queue)
    def test_route_for_task(self):
        expand = E(conf.QUEUES)
        route = routes.MapRoute({"celery.ping": b_queue})
        self.assertDictContainsSubset(b_queue,
                             expand(route.route_for_task("celery.ping")))
        self.assertIsNone(route.route_for_task("celery.awesome"))

    def test_expand_route_not_found(self):
        expand = E(conf.QUEUES)
        route = routes.MapRoute({"a": "x"})
        self.assertRaises(RouteNotFound, expand, route.route_for_task("a"))


class test_lookup_route(unittest.TestCase):

    @with_queues(foo=a_queue, bar=b_queue)
    def test_lookup_takes_first(self):
        R = routes.prepare(({"celery.ping": "bar"},
                            {"celery.ping": "foo"}))
        router = routes.Router(R, conf.QUEUES)
        self.assertDictContainsSubset(b_queue,
                router.route({}, "celery.ping",
                    args=[1, 2], kwargs={}))

    @with_queues(foo=a_queue, bar=b_queue)
    def test_lookup_paths_traversed(self):
        R = routes.prepare(({"celery.xaza": "bar"},
                            {"celery.ping": "foo"}))
        router = routes.Router(R, conf.QUEUES)
        self.assertDictContainsSubset(a_queue,
                router.route({}, "celery.ping",
                    args=[1, 2], kwargs={}))
        self.assertEqual(router.route({}, "celery.poza"), {})
