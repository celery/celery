from __future__ import absolute_import, unicode_literals

import pytest

from case import ContextMock, MagicMock, Mock

from celery._state import _task_stack
from celery.canvas import (
    Signature,
    chain,
    group,
    chord,
    signature,
    xmap,
    xstarmap,
    chunks,
    _maybe_group,
    maybe_signature,
    maybe_unroll_group,
    _seq_concat_seq,
)
from celery.result import EagerResult

SIG = Signature({
    'task': 'TASK',
    'args': ('A1',),
    'kwargs': {'K1': 'V1'},
    'options': {'task_id': 'TASK_ID'},
    'subtask_type': ''},
)


class test_maybe_unroll_group:

    def test_when_no_len_and_no_length_hint(self):
        g = MagicMock(name='group')
        g.tasks.__len__.side_effect = TypeError()
        g.tasks.__length_hint__ = Mock()
        g.tasks.__length_hint__.return_value = 0
        assert maybe_unroll_group(g) is g
        g.tasks.__length_hint__.side_effect = AttributeError()
        assert maybe_unroll_group(g) is g


@pytest.mark.parametrize('a,b,expected', [
    ((1, 2, 3), [4, 5], (1, 2, 3, 4, 5)),
    ((1, 2), [3, 4, 5], [1, 2, 3, 4, 5]),
    ([1, 2, 3], (4, 5), [1, 2, 3, 4, 5]),
    ([1, 2], (3, 4, 5), (1, 2, 3, 4, 5)),
])
def test_seq_concat_seq(a, b, expected):
    res = _seq_concat_seq(a, b)
    assert type(res) is type(expected)  # noqa
    assert res == expected


class CanvasCase:

    def setup(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        self.add = add

        @self.app.task(shared=False)
        def mul(x, y):
            return x * y
        self.mul = mul

        @self.app.task(shared=False)
        def div(x, y):
            return x / y
        self.div = div


class test_Signature(CanvasCase):

    def test_getitem_property_class(self):
        assert Signature.task
        assert Signature.args
        assert Signature.kwargs
        assert Signature.options
        assert Signature.subtask_type

    def test_getitem_property(self):
        assert SIG.task == 'TASK'
        assert SIG.args == ('A1',)
        assert SIG.kwargs == {'K1': 'V1'}
        assert SIG.options == {'task_id': 'TASK_ID'}
        assert SIG.subtask_type == ''

    def test_call(self):
        x = Signature('foo', (1, 2), {'arg1': 33}, app=self.app)
        x.type = Mock(name='type')
        x(3, 4, arg2=66)
        x.type.assert_called_with(3, 4, 1, 2, arg1=33, arg2=66)

    def test_link_on_scalar(self):
        x = Signature('TASK', link=Signature('B'))
        assert x.options['link']
        x.link(Signature('C'))
        assert isinstance(x.options['link'], list)
        assert Signature('B') in x.options['link']
        assert Signature('C') in x.options['link']

    def test_json(self):
        x = Signature('TASK', link=Signature('B', app=self.app), app=self.app)
        assert x.__json__() == dict(x)

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce(self):
        x = Signature('TASK', (2, 4), app=self.app)
        fun, args = x.__reduce__()
        assert fun(*args) == x

    def test_replace(self):
        x = Signature('TASK', ('A'), {})
        assert x.replace(args=('B',)).args == ('B',)
        assert x.replace(kwargs={'FOO': 'BAR'}).kwargs == {
            'FOO': 'BAR',
        }
        assert x.replace(options={'task_id': '123'}).options == {
            'task_id': '123',
        }

    def test_set(self):
        assert Signature('TASK', x=1).set(task_id='2').options == {
            'x': 1, 'task_id': '2',
        }

    def test_link(self):
        x = signature(SIG)
        x.link(SIG)
        x.link(SIG)
        assert SIG in x.options['link']
        assert len(x.options['link']) == 1

    def test_link_error(self):
        x = signature(SIG)
        x.link_error(SIG)
        x.link_error(SIG)
        assert SIG in x.options['link_error']
        assert len(x.options['link_error']) == 1

    def test_flatten_links(self):
        tasks = [self.add.s(2, 2), self.mul.s(4), self.div.s(2)]
        tasks[0].link(tasks[1])
        tasks[1].link(tasks[2])
        assert tasks[0].flatten_links() == tasks

    def test_OR(self):
        x = self.add.s(2, 2) | self.mul.s(4)
        assert isinstance(x, chain)
        y = self.add.s(4, 4) | self.div.s(2)
        z = x | y
        assert isinstance(y, chain)
        assert isinstance(z, chain)
        assert len(z.tasks) == 4
        with pytest.raises(TypeError):
            x | 10
        ax = self.add.s(2, 2) | (self.add.s(4) | self.add.s(8))
        assert isinstance(ax, chain)
        assert len(ax.tasks), 3 == 'consolidates chain to chain'

    def test_INVERT(self):
        x = self.add.s(2, 2)
        x.apply_async = Mock()
        x.apply_async.return_value = Mock()
        x.apply_async.return_value.get = Mock()
        x.apply_async.return_value.get.return_value = 4
        assert ~x == 4
        x.apply_async.assert_called()

    def test_merge_immutable(self):
        x = self.add.si(2, 2, foo=1)
        args, kwargs, options = x._merge((4,), {'bar': 2}, {'task_id': 3})
        assert args == (2, 2)
        assert kwargs == {'foo': 1}
        assert options == {'task_id': 3}

    def test_set_immutable(self):
        x = self.add.s(2, 2)
        assert not x.immutable
        x.set(immutable=True)
        assert x.immutable
        x.set(immutable=False)
        assert not x.immutable

    def test_election(self):
        x = self.add.s(2, 2)
        x.freeze('foo')
        x.type.app.control = Mock()
        r = x.election()
        x.type.app.control.election.assert_called()
        assert r.id == 'foo'

    def test_AsyncResult_when_not_registered(self):
        s = signature('xxx.not.registered', app=self.app)
        assert s.AsyncResult

    def test_apply_async_when_not_registered(self):
        s = signature('xxx.not.registered', app=self.app)
        assert s._apply_async


class test_xmap_xstarmap(CanvasCase):

    def test_apply(self):
        for type, attr in [(xmap, 'map'), (xstarmap, 'starmap')]:
            args = [(i, i) for i in range(10)]
            s = getattr(self.add, attr)(args)
            s.type = Mock()

            s.apply_async(foo=1)
            s.type.apply_async.assert_called_with(
                (), {'task': self.add.s(), 'it': args}, foo=1,
                route_name=self.add.name,
            )

            assert type.from_dict(dict(s)) == s
            assert repr(s)


class test_chunks(CanvasCase):

    def test_chunks(self):
        x = self.add.chunks(range(100), 10)
        assert dict(chunks.from_dict(dict(x), app=self.app)) == dict(x)

        assert x.group()
        assert len(x.group().tasks) == 10

        x.group = Mock()
        gr = x.group.return_value = Mock()

        x.apply_async()
        gr.apply_async.assert_called_with((), {}, route_name=self.add.name)
        gr.apply_async.reset_mock()
        x()
        gr.apply_async.assert_called_with((), {}, route_name=self.add.name)

        self.app.conf.task_always_eager = True
        chunks.apply_chunks(app=self.app, **x['kwargs'])


class test_chain(CanvasCase):

    def test_clone_preserves_state(self):
        x = chain(self.add.s(i, i) for i in range(10))
        assert x.clone().tasks == x.tasks
        assert x.clone().kwargs == x.kwargs
        assert x.clone().args == x.args

    def test_repr(self):
        x = self.add.s(2, 2) | self.add.s(2)
        assert repr(x) == '%s(2, 2) | %s(2)' % (self.add.name, self.add.name)

    def test_apply_async(self):
        c = self.add.s(2, 2) | self.add.s(4) | self.add.s(8)
        result = c.apply_async()
        assert result.parent
        assert result.parent.parent
        assert result.parent.parent.parent is None

    def test_group_to_chord__freeze_parent_id(self):
        def using_freeze(c):
            c.freeze(parent_id='foo', root_id='root')
            return c._frozen[0]
        self.assert_group_to_chord_parent_ids(using_freeze)

    def assert_group_to_chord_parent_ids(self, freezefun):
        c = (
            self.add.s(5, 5) |
            group([self.add.s(i, i) for i in range(5)], app=self.app) |
            self.add.si(10, 10) |
            self.add.si(20, 20) |
            self.add.si(30, 30)
        )
        tasks = freezefun(c)
        assert tasks[-1].parent_id == 'foo'
        assert tasks[-1].root_id == 'root'
        assert tasks[-2].parent_id == tasks[-1].id
        assert tasks[-2].root_id == 'root'
        assert tasks[-2].body.parent_id == tasks[-2].tasks.id
        assert tasks[-2].body.parent_id == tasks[-2].id
        assert tasks[-2].body.root_id == 'root'
        assert tasks[-2].tasks.tasks[0].parent_id == tasks[-1].id
        assert tasks[-2].tasks.tasks[0].root_id == 'root'
        assert tasks[-2].tasks.tasks[1].parent_id == tasks[-1].id
        assert tasks[-2].tasks.tasks[1].root_id == 'root'
        assert tasks[-2].tasks.tasks[2].parent_id == tasks[-1].id
        assert tasks[-2].tasks.tasks[2].root_id == 'root'
        assert tasks[-2].tasks.tasks[3].parent_id == tasks[-1].id
        assert tasks[-2].tasks.tasks[3].root_id == 'root'
        assert tasks[-2].tasks.tasks[4].parent_id == tasks[-1].id
        assert tasks[-2].tasks.tasks[4].root_id == 'root'
        assert tasks[-3].parent_id == tasks[-2].body.id
        assert tasks[-3].root_id == 'root'
        assert tasks[-4].parent_id == tasks[-3].id
        assert tasks[-4].root_id == 'root'

    def test_splices_chains(self):
        c = chain(
            self.add.s(5, 5),
            chain(self.add.s(6), self.add.s(7), self.add.s(8), app=self.app),
            app=self.app,
        )
        c.freeze()
        tasks, _ = c._frozen
        assert len(tasks) == 4

    def test_from_dict_no_tasks(self):
        assert chain.from_dict(dict(chain(app=self.app)), app=self.app)

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_app_falls_back_to_default(self):
        from celery._state import current_app
        assert chain().app is current_app

    def test_handles_dicts(self):
        c = chain(
            self.add.s(5, 5), dict(self.add.s(8)), app=self.app,
        )
        c.freeze()
        tasks, _ = c._frozen
        for task in tasks:
            assert isinstance(task, Signature)
            assert task.app is self.app

    def test_group_to_chord(self):
        c = (
            self.add.s(5) |
            group([self.add.s(i, i) for i in range(5)], app=self.app) |
            self.add.s(10) |
            self.add.s(20) |
            self.add.s(30)
        )
        c._use_link = True
        tasks, results = c.prepare_steps((), c.tasks)

        assert tasks[-1].args[0] == 5
        assert isinstance(tasks[-2], chord)
        assert len(tasks[-2].tasks) == 5
        assert tasks[-2].parent_id == tasks[-1].id
        assert tasks[-2].root_id == tasks[-1].id
        assert tasks[-2].body.args[0] == 10
        assert tasks[-2].body.parent_id == tasks[-2].id

        assert tasks[-3].args[0] == 20
        assert tasks[-3].root_id == tasks[-1].id
        assert tasks[-3].parent_id == tasks[-2].body.id

        assert tasks[-4].args[0] == 30
        assert tasks[-4].parent_id == tasks[-3].id
        assert tasks[-4].root_id == tasks[-1].id

        assert tasks[-2].body.options['link']
        assert tasks[-2].body.options['link'][0].options['link']

        c2 = self.add.s(2, 2) | group(self.add.s(i, i) for i in range(10))
        c2._use_link = True
        tasks2, _ = c2.prepare_steps((), c2.tasks)
        assert isinstance(tasks2[0], group)

    def test_group_to_chord__protocol_2__or(self):
        c = (
            group([self.add.s(i, i) for i in range(5)], app=self.app) |
            self.add.s(10) |
            self.add.s(20) |
            self.add.s(30)
        )
        assert isinstance(c, chord)

    def test_group_to_chord__protocol_2(self):
        c = chain(
            group([self.add.s(i, i) for i in range(5)], app=self.app),
            self.add.s(10),
            self.add.s(20),
            self.add.s(30)
        )
        c._use_link = False
        tasks, _ = c.prepare_steps((), c.tasks)
        assert isinstance(tasks[-1], chord)

        c2 = self.add.s(2, 2) | group(self.add.s(i, i) for i in range(10))
        c2._use_link = False
        tasks2, _ = c2.prepare_steps((), c2.tasks)
        assert isinstance(tasks2[0], group)

    def test_apply_options(self):

        class static(Signature):

            def clone(self, *args, **kwargs):
                return self

        def s(*args, **kwargs):
            return static(self.add, args, kwargs, type=self.add, app=self.app)

        c = s(2, 2) | s(4) | s(8)
        r1 = c.apply_async(task_id='some_id')
        assert r1.id == 'some_id'

        c.apply_async(group_id='some_group_id')
        assert c.tasks[-1].options['group_id'] == 'some_group_id'

        c.apply_async(chord='some_chord_id')
        assert c.tasks[-1].options['chord'] == 'some_chord_id'

        c.apply_async(link=[s(32)])
        assert c.tasks[-1].options['link'] == [s(32)]

        c.apply_async(link_error=[s('error')])
        for task in c.tasks:
            assert task.options['link_error'] == [s('error')]

    def test_reverse(self):
        x = self.add.s(2, 2) | self.add.s(2)
        assert isinstance(signature(x), chain)
        assert isinstance(signature(dict(x)), chain)

    def test_always_eager(self):
        self.app.conf.task_always_eager = True
        assert ~(self.add.s(4, 4) | self.add.s(8)) == 16

    def test_apply(self):
        x = chain(self.add.s(4, 4), self.add.s(8), self.add.s(10))
        res = x.apply()
        assert isinstance(res, EagerResult)
        assert res.get() == 26

        assert res.parent.get() == 16
        assert res.parent.parent.get() == 8
        assert res.parent.parent.parent is None

    def test_empty_chain_returns_none(self):
        assert chain(app=self.app)() is None
        assert chain(app=self.app).apply_async() is None

    def test_root_id_parent_id(self):
        self.app.conf.task_protocol = 2
        c = chain(self.add.si(i, i) for i in range(4))
        c.freeze()
        tasks, _ = c._frozen
        for i, task in enumerate(tasks):
            assert task.root_id == tasks[-1].id
            try:
                assert task.parent_id == tasks[i + 1].id
            except IndexError:
                assert i == len(tasks) - 1
            else:
                valid_parents = i
        assert valid_parents == len(tasks) - 2

        self.assert_sent_with_ids(tasks[-1], tasks[-1].id, 'foo',
                                  parent_id='foo')
        assert tasks[-2].options['parent_id']
        self.assert_sent_with_ids(tasks[-2], tasks[-1].id, tasks[-1].id)
        self.assert_sent_with_ids(tasks[-3], tasks[-1].id, tasks[-2].id)
        self.assert_sent_with_ids(tasks[-4], tasks[-1].id, tasks[-3].id)

    def assert_sent_with_ids(self, task, rid, pid, **options):
        self.app.amqp.send_task_message = Mock(name='send_task_message')
        self.app.backend = Mock()
        self.app.producer_or_acquire = ContextMock()

        task.apply_async(**options)
        self.app.amqp.send_task_message.assert_called()
        message = self.app.amqp.send_task_message.call_args[0][2]
        assert message.headers['parent_id'] == pid
        assert message.headers['root_id'] == rid

    def test_call_no_tasks(self):
        x = chain()
        assert not x()

    def test_call_with_tasks(self):
        x = self.add.s(2, 2) | self.add.s(4)
        x.apply_async = Mock()
        x(2, 2, foo=1)
        x.apply_async.assert_called_with((2, 2), {'foo': 1})

    def test_from_dict_no_args__with_args(self):
        x = dict(self.add.s(2, 2) | self.add.s(4))
        x['args'] = None
        assert isinstance(chain.from_dict(x), chain)
        x['args'] = (2,)
        assert isinstance(chain.from_dict(x), chain)

    def test_accepts_generator_argument(self):
        x = chain(self.add.s(i) for i in range(10))
        assert x.tasks[0].type, self.add
        assert x.type


class test_group(CanvasCase):

    def test_repr(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        assert repr(x)

    def test_reverse(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        assert isinstance(signature(x), group)
        assert isinstance(signature(dict(x)), group)

    def test_group_with_group_argument(self):
        g1 = group(self.add.s(2, 2), self.add.s(4, 4), app=self.app)
        g2 = group(g1, app=self.app)
        assert g2.tasks is g1.tasks

    def test_maybe_group_sig(self):
        assert _maybe_group(self.add.s(2, 2), self.app) == [self.add.s(2, 2)]

    def test_apply(self):
        x = group([self.add.s(4, 4), self.add.s(8, 8)])
        res = x.apply()
        assert res.get(), [8 == 16]

    def test_apply_async(self):
        x = group([self.add.s(4, 4), self.add.s(8, 8)])
        x.apply_async()

    def test_prepare_with_dict(self):
        x = group([self.add.s(4, 4), dict(self.add.s(8, 8))], app=self.app)
        x.apply_async()

    def test_group_in_group(self):
        g1 = group(self.add.s(2, 2), self.add.s(4, 4), app=self.app)
        g2 = group(self.add.s(8, 8), g1, self.add.s(16, 16), app=self.app)
        g2.apply_async()

    def test_set_immutable(self):
        g1 = group(Mock(name='t1'), Mock(name='t2'), app=self.app)
        g1.set_immutable(True)
        for task in g1.tasks:
            task.set_immutable.assert_called_with(True)

    def test_link(self):
        g1 = group(Mock(name='t1'), Mock(name='t2'), app=self.app)
        sig = Mock(name='sig')
        g1.link(sig)
        g1.tasks[0].link.assert_called_with(sig.clone().set(immutable=True))

    def test_link_error(self):
        g1 = group(Mock(name='t1'), Mock(name='t2'), app=self.app)
        sig = Mock(name='sig')
        g1.link_error(sig)
        g1.tasks[0].link_error.assert_called_with(
            sig.clone().set(immutable=True),
        )

    def test_apply_empty(self):
        x = group(app=self.app)
        x.apply()
        res = x.apply_async()
        assert res
        assert not res.results

    def test_apply_async_with_parent(self):
        _task_stack.push(self.add)
        try:
            self.add.push_request(called_directly=False)
            try:
                assert not self.add.request.children
                x = group([self.add.s(4, 4), self.add.s(8, 8)])
                res = x()
                assert self.add.request.children
                assert res in self.add.request.children
                assert len(self.add.request.children) == 1
            finally:
                self.add.pop_request()
        finally:
            _task_stack.pop()

    def test_from_dict(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        x['args'] = (2, 2)
        assert group.from_dict(dict(x))
        x['args'] = None
        assert group.from_dict(dict(x))

    def test_call_empty_group(self):
        x = group(app=self.app)
        assert not len(x())
        x.delay()
        x.apply_async()
        x()

    def test_skew(self):
        g = group([self.add.s(i, i) for i in range(10)])
        g.skew(start=1, stop=10, step=1)
        for i, task in enumerate(g.tasks):
            assert task.options['countdown'] == i + 1

    def test_iter(self):
        g = group([self.add.s(i, i) for i in range(10)])
        assert list(iter(g)) == g.tasks

    @staticmethod
    def helper_test_get_delay(result):
        import time
        t0 = time.time()
        while not result.ready():
            time.sleep(0.01)
            if time.time() - t0 > 1:
                return None
        return result.get()

    def test_kwargs_direct(self):
        res = [self.add(x=1, y=1), self.add(x=1, y=1)]
        assert res == [2, 2]

    def test_kwargs_apply(self):
        x = group([self.add.s(), self.add.s()])
        res = x.apply(kwargs=dict(x=1, y=1)).get()
        assert res == [2, 2]

    def test_kwargs_apply_async(self):
        self.app.conf.task_always_eager = True
        x = group([self.add.s(), self.add.s()])
        res = self.helper_test_get_delay(x.apply_async(kwargs=dict(x=1, y=1)))
        assert res == [2, 2]

    def test_kwargs_delay(self):
        self.app.conf.task_always_eager = True
        x = group([self.add.s(), self.add.s()])
        res = self.helper_test_get_delay(x.delay(x=1, y=1))
        assert res == [2, 2]

    def test_kwargs_delay_partial(self):
        self.app.conf.task_always_eager = True
        x = group([self.add.s(1), self.add.s(x=1)])
        res = self.helper_test_get_delay(x.delay(y=1))
        assert res == [2, 2]


class test_chord(CanvasCase):

    def test_reverse(self):
        x = chord([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        assert isinstance(signature(x), chord)
        assert isinstance(signature(dict(x)), chord)

    def test_clone_clones_body(self):
        x = chord([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        y = x.clone()
        assert x.kwargs['body'] is not y.kwargs['body']
        y.kwargs.pop('body')
        z = y.clone()
        assert z.kwargs.get('body') is None

    def test_argument_is_group(self):
        x = chord(group(self.add.s(2, 2), self.add.s(4, 4), app=self.app))
        assert x.tasks

    def test_set_parent_id(self):
        x = chord(group(self.add.s(2, 2)))
        x.tasks = [self.add.s(2, 2)]
        x.set_parent_id('pid')

    def test_app_when_app(self):
        app = Mock(name='app')
        x = chord([self.add.s(4, 4)], app=app)
        assert x.app is app

    def test_app_when_app_in_task(self):
        t1 = Mock(name='t1')
        t2 = Mock(name='t2')
        x = chord([t1, self.add.s(4, 4)])
        assert x.app is x.tasks[0].app
        t1.app = None
        x = chord([t1], body=t2)
        assert x.app is t2._app

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_app_fallback_to_current(self):
        from celery._state import current_app
        t1 = Mock(name='t1')
        t1.app = t1._app = None
        x = chord([t1], body=t1)
        assert x.app is current_app

    def test_set_immutable(self):
        x = chord([Mock(name='t1'), Mock(name='t2')], app=self.app)
        x.set_immutable(True)

    def test_links_to_body(self):
        x = chord([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        x.link(self.div.s(2))
        assert not x.options.get('link')
        assert x.kwargs['body'].options['link']

        x.link_error(self.div.s(2))
        assert not x.options.get('link_error')
        assert x.kwargs['body'].options['link_error']

        assert x.tasks
        assert x.body

    def test_repr(self):
        x = chord([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        assert repr(x)
        x.kwargs['body'] = None
        assert 'without body' in repr(x)

    def test_freeze_tasks_is_not_group(self):
        x = chord([self.add.s(2, 2)], body=self.add.s(), app=self.app)
        x.freeze()
        x.tasks = [self.add.s(2, 2)]
        x.freeze()


class test_maybe_signature(CanvasCase):

    def test_is_None(self):
        assert maybe_signature(None, app=self.app) is None

    def test_is_dict(self):
        assert isinstance(maybe_signature(dict(self.add.s()), app=self.app),
                          Signature)

    def test_when_sig(self):
        s = self.add.s()
        assert maybe_signature(s, app=self.app) is s
