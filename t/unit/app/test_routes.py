from __future__ import absolute_import, unicode_literals

import pytest
from case import ANY, Mock
from kombu import Exchange, Queue
from kombu.utils.functional import maybe_evaluate

from celery.app import routes
from celery.exceptions import QueueNotFound
from celery.five import items
from celery.utils.imports import qualname


def Router(app, *args, **kwargs):
    return routes.Router(*args, app=app, **kwargs)


def E(app, queues):
    def expand(answer):
        return Router(app, [], queues).expand_destination(answer)
    return expand


def set_queues(app, **queues):
    app.conf.task_queues = queues
    app.amqp.queues = app.amqp.Queues(queues)


class RouteCase:

    def setup(self):
        self.a_queue = {
            'exchange': 'fooexchange',
            'exchange_type': 'fanout',
            'routing_key': 'xuzzy',
        }
        self.b_queue = {
            'exchange': 'barexchange',
            'exchange_type': 'topic',
            'routing_key': 'b.b.#',
        }
        self.d_queue = {
            'exchange': self.app.conf.task_default_exchange,
            'exchange_type': self.app.conf.task_default_exchange_type,
            'routing_key': self.app.conf.task_default_routing_key,
        }

        @self.app.task(shared=False)
        def mytask(*args, **kwargs):
            pass
        self.mytask = mytask

    def assert_routes_to_queue(self, queue, router, name,
                               args=[], kwargs={}, options={}):
        assert router.route(options, name, args, kwargs)['queue'].name == queue

    def assert_routes_to_default_queue(self, router, name, *args, **kwargs):
        self.assert_routes_to_queue(
            self.app.conf.task_default_queue, router, name, *args, **kwargs)


class test_MapRoute(RouteCase):

    def test_route_for_task_expanded_route(self):
        set_queues(self.app, foo=self.a_queue, bar=self.b_queue)
        expand = E(self.app, self.app.amqp.queues)
        route = routes.MapRoute({self.mytask.name: {'queue': 'foo'}})
        assert expand(route(self.mytask.name))['queue'].name == 'foo'
        assert route('celery.awesome') is None

    def test_route_for_task(self):
        set_queues(self.app, foo=self.a_queue, bar=self.b_queue)
        expand = E(self.app, self.app.amqp.queues)
        route = routes.MapRoute({self.mytask.name: self.b_queue})
        eroute = expand(route(self.mytask.name))
        for key, value in items(self.b_queue):
            assert eroute[key] == value
        assert route('celery.awesome') is None

    def test_route_for_task__glob(self):
        from re import compile

        route = routes.MapRoute([
            ('proj.tasks.*', 'routeA'),
            ('demoapp.tasks.bar.*', {'exchange': 'routeB'}),
            (compile(r'(video|image)\.tasks\..*'), {'queue': 'media'}),
        ])
        assert route('proj.tasks.foo') == {'queue': 'routeA'}
        assert route('demoapp.tasks.bar.moo') == {'exchange': 'routeB'}
        assert route('video.tasks.foo') == {'queue': 'media'}
        assert route('image.tasks.foo') == {'queue': 'media'}
        assert route('demoapp.foo.bar.moo') is None

    def test_expand_route_not_found(self):
        expand = E(self.app, self.app.amqp.Queues(
                   self.app.conf.task_queues, False))
        route = routes.MapRoute({'a': {'queue': 'x'}})
        with pytest.raises(QueueNotFound):
            expand(route('a'))


class test_lookup_route(RouteCase):

    def test_init_queues(self):
        router = Router(self.app, queues=None)
        assert router.queues == {}

    def test_lookup_takes_first(self):
        set_queues(self.app, foo=self.a_queue, bar=self.b_queue)
        R = routes.prepare(({self.mytask.name: {'queue': 'bar'}},
                            {self.mytask.name: {'queue': 'foo'}}))
        router = Router(self.app, R, self.app.amqp.queues)
        self.assert_routes_to_queue('bar', router, self.mytask.name)

    def test_expands_queue_in_options(self):
        set_queues(self.app)
        R = routes.prepare(())
        router = Router(
            self.app, R, self.app.amqp.queues, create_missing=True,
        )
        # apply_async forwards all arguments, even exchange=None etc,
        # so need to make sure it's merged correctly.
        route = router.route(
            {'queue': 'testq',
             'exchange': None,
             'routing_key': None,
             'immediate': False},
            self.mytask.name,
            args=[1, 2], kwargs={},
        )
        assert route['queue'].name == 'testq'
        assert route['queue'].exchange == Exchange('testq')
        assert route['queue'].routing_key == 'testq'
        assert route['immediate'] is False

    def test_expand_destination_string(self):
        set_queues(self.app, foo=self.a_queue, bar=self.b_queue)
        x = Router(self.app, {}, self.app.amqp.queues)
        dest = x.expand_destination('foo')
        assert dest['queue'].name == 'foo'

    def test_expand_destination__Queue(self):
        queue = Queue('foo')
        x = Router(self.app, {}, self.app.amqp.queues)
        dest = x.expand_destination({'queue': queue})
        assert dest['queue'] is queue

    def test_lookup_paths_traversed(self):
        self.simple_queue_setup()
        R = routes.prepare((
            {'celery.xaza': {'queue': 'bar'}},
            {self.mytask.name: {'queue': 'foo'}}
        ))
        router = Router(self.app, R, self.app.amqp.queues)
        self.assert_routes_to_queue('foo', router, self.mytask.name)
        self.assert_routes_to_default_queue(router, 'celery.poza')

    def test_compat_router_class(self):
        self.simple_queue_setup()
        R = routes.prepare((
            TestRouter(),
        ))
        router = Router(self.app, R, self.app.amqp.queues)
        self.assert_routes_to_queue('bar', router, 'celery.xaza')
        self.assert_routes_to_default_queue(router, 'celery.poza')

    def test_router_fun__called_with(self):
        self.simple_queue_setup()
        step = Mock(spec=['__call__'])
        step.return_value = None
        R = routes.prepare([step])
        router = Router(self.app, R, self.app.amqp.queues)
        self.mytask.apply_async((2, 2), {'kw': 3}, router=router, priority=3)
        step.assert_called_with(
            self.mytask.name, (2, 2), {'kw': 3}, ANY,
            task=self.mytask,
        )
        options = step.call_args[0][3]
        assert options['priority'] == 3

    def test_compat_router_classes__called_with(self):
        self.simple_queue_setup()
        step = Mock(spec=['route_for_task'])
        step.route_for_task.return_value = None
        R = routes.prepare([step])
        router = Router(self.app, R, self.app.amqp.queues)
        self.mytask.apply_async((2, 2), {'kw': 3}, router=router, priority=3)
        step.route_for_task.assert_called_with(
            self.mytask.name, (2, 2), {'kw': 3},
        )

    def simple_queue_setup(self):
        set_queues(
            self.app, foo=self.a_queue, bar=self.b_queue,
            **{self.app.conf.task_default_queue: self.d_queue})


class TestRouter(object):

    def route_for_task(self, task, args, kwargs):
        if task == 'celery.xaza':
            return 'bar'


class test_prepare:

    def test_prepare(self):
        o = object()
        R = [
            {'foo': 'bar'},
            qualname(TestRouter),
            o,
        ]
        p = routes.prepare(R)
        assert isinstance(p[0], routes.MapRoute)
        assert isinstance(maybe_evaluate(p[1]), TestRouter)
        assert p[2] is o

        assert routes.prepare(o) == [o]

    def test_prepare_item_is_dict(self):
        R = {'foo': 'bar'}
        p = routes.prepare(R)
        assert isinstance(p[0], routes.MapRoute)
