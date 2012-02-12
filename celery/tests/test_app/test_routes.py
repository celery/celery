from __future__ import absolute_import
from __future__ import with_statement

from functools import wraps

from celery import routes
from celery import current_app
from celery.exceptions import QueueNotFound
from celery.task import task
from celery.utils import maybe_promise
from celery.tests.utils import Case


@task
def mytask():
    pass


def E(queues):
    def expand(answer):
        return routes.Router([], queues).expand_destination(answer)
    return expand


def with_queues(**queues):

    def patch_fun(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            app = current_app
            prev_queues = app.conf.CELERY_QUEUES
            prev_Queues = app.amqp.queues
            app.conf.CELERY_QUEUES = queues
            app.amqp.queues = app.amqp.Queues(queues)
            try:
                return fun(*args, **kwargs)
            finally:
                app.conf.CELERY_QUEUES = prev_queues
                app.amqp.queues = prev_Queues
        return __inner
    return patch_fun


a_queue = {"exchange": "fooexchange",
           "exchange_type": "fanout",
               "binding_key": "xuzzy"}
b_queue = {"exchange": "barexchange",
           "exchange_type": "topic",
           "binding_key": "b.b.#"}
d_queue = {"exchange": current_app.conf.CELERY_DEFAULT_EXCHANGE,
           "exchange_type": current_app.conf.CELERY_DEFAULT_EXCHANGE_TYPE,
           "routing_key": current_app.conf.CELERY_DEFAULT_ROUTING_KEY}


class test_MapRoute(Case):

    @with_queues(foo=a_queue, bar=b_queue)
    def test_route_for_task_expanded_route(self):
        expand = E(current_app.conf.CELERY_QUEUES)
        route = routes.MapRoute({mytask.name: {"queue": "foo"}})
        self.assertDictContainsSubset(a_queue,
                             expand(route.route_for_task(mytask.name)))
        self.assertIsNone(route.route_for_task("celery.awesome"))

    @with_queues(foo=a_queue, bar=b_queue)
    def test_route_for_task(self):
        expand = E(current_app.conf.CELERY_QUEUES)
        route = routes.MapRoute({mytask.name: b_queue})
        self.assertDictContainsSubset(b_queue,
                             expand(route.route_for_task(mytask.name)))
        self.assertIsNone(route.route_for_task("celery.awesome"))

    def test_expand_route_not_found(self):
        expand = E(current_app.conf.CELERY_QUEUES)
        route = routes.MapRoute({"a": {"queue": "x"}})
        with self.assertRaises(QueueNotFound):
            expand(route.route_for_task("a"))


class test_lookup_route(Case):

    def test_init_queues(self):
        router = routes.Router(queues=None)
        self.assertDictEqual(router.queues, {})

    @with_queues(foo=a_queue, bar=b_queue)
    def test_lookup_takes_first(self):
        R = routes.prepare(({mytask.name: {"queue": "bar"}},
                            {mytask.name: {"queue": "foo"}}))
        router = routes.Router(R, current_app.conf.CELERY_QUEUES)
        self.assertDictContainsSubset(b_queue,
                router.route({}, mytask.name,
                    args=[1, 2], kwargs={}))

    @with_queues()
    def test_expands_queue_in_options(self):
        R = routes.prepare(())
        router = routes.Router(R, current_app.conf.CELERY_QUEUES,
                               create_missing=True)
        # apply_async forwards all arguments, even exchange=None etc,
        # so need to make sure it's merged correctly.
        route = router.route({"queue": "testq",
                              "exchange": None,
                              "routing_key": None,
                              "immediate": False},
                             mytask.name,
                             args=[1, 2], kwargs={})
        self.assertDictContainsSubset({"exchange": "testq",
                                       "routing_key": "testq",
                                       "immediate": False},
                                       route)
        self.assertIn("queue", route)

    @with_queues(foo=a_queue, bar=b_queue)
    def test_expand_destaintion_string(self):
        x = routes.Router({}, current_app.conf.CELERY_QUEUES)
        dest = x.expand_destination("foo")
        self.assertEqual(dest["exchange"], "fooexchange")

    @with_queues(foo=a_queue, bar=b_queue, **{
        current_app.conf.CELERY_DEFAULT_QUEUE: d_queue})
    def test_lookup_paths_traversed(self):
        R = routes.prepare(({"celery.xaza": {"queue": "bar"}},
                            {mytask.name: {"queue": "foo"}}))
        router = routes.Router(R, current_app.amqp.queues)
        self.assertDictContainsSubset(a_queue,
                router.route({}, mytask.name,
                    args=[1, 2], kwargs={}))
        self.assertEqual(router.route({}, "celery.poza"),
                dict(d_queue, queue=current_app.conf.CELERY_DEFAULT_QUEUE))


class test_prepare(Case):

    def test_prepare(self):
        from celery.datastructures import LRUCache
        o = object()
        R = [{"foo": "bar"},
                  "celery.datastructures.LRUCache",
                  o]
        p = routes.prepare(R)
        self.assertIsInstance(p[0], routes.MapRoute)
        self.assertIsInstance(maybe_promise(p[1]), LRUCache)
        self.assertIs(p[2], o)

        self.assertEqual(routes.prepare(o), [o])

    def test_prepare_item_is_dict(self):
        R = {"foo": "bar"}
        p = routes.prepare(R)
        self.assertIsInstance(p[0], routes.MapRoute)
