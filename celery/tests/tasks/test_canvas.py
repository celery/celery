from __future__ import absolute_import, unicode_literals

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
)
from celery.result import EagerResult

from celery.tests.case import (
    AppCase, ContextMock, MagicMock, Mock, depends_on_current_app,
)

SIG = Signature({
    'task': 'TASK',
    'args': ('A1',),
    'kwargs': {'K1': 'V1'},
    'options': {'task_id': 'TASK_ID'},
    'subtask_type': ''},
)


class test_maybe_unroll_group(AppCase):

    def test_when_no_len_and_no_length_hint(self):
        g = MagicMock(name='group')
        g.tasks.__len__.side_effect = TypeError()
        g.tasks.__length_hint__ = Mock()
        g.tasks.__length_hint__.return_value = 0
        self.assertIs(maybe_unroll_group(g), g)
        g.tasks.__length_hint__.side_effect = AttributeError()
        self.assertIs(maybe_unroll_group(g), g)


class CanvasCase(AppCase):

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
        self.assertTrue(Signature.task)
        self.assertTrue(Signature.args)
        self.assertTrue(Signature.kwargs)
        self.assertTrue(Signature.options)
        self.assertTrue(Signature.subtask_type)

    def test_getitem_property(self):
        self.assertEqual(SIG.task, 'TASK')
        self.assertEqual(SIG.args, ('A1',))
        self.assertEqual(SIG.kwargs, {'K1': 'V1'})
        self.assertEqual(SIG.options, {'task_id': 'TASK_ID'})
        self.assertEqual(SIG.subtask_type, '')

    def test_call(self):
        x = Signature('foo', (1, 2), {'arg1': 33}, app=self.app)
        x.type = Mock(name='type')
        x(3, 4, arg2=66)
        x.type.assert_called_with(3, 4, 1, 2, arg1=33, arg2=66)

    def test_link_on_scalar(self):
        x = Signature('TASK', link=Signature('B'))
        self.assertTrue(x.options['link'])
        x.link(Signature('C'))
        self.assertIsInstance(x.options['link'], list)
        self.assertIn(Signature('B'), x.options['link'])
        self.assertIn(Signature('C'), x.options['link'])

    def test_json(self):
        x = Signature('TASK', link=Signature('B', app=self.app), app=self.app)
        self.assertDictEqual(x.__json__(), dict(x))

    @depends_on_current_app
    def test_reduce(self):
        x = Signature('TASK', (2, 4), app=self.app)
        fun, args = x.__reduce__()
        self.assertEqual(fun(*args), x)

    def test_replace(self):
        x = Signature('TASK', ('A'), {})
        self.assertTupleEqual(x.replace(args=('B',)).args, ('B',))
        self.assertDictEqual(
            x.replace(kwargs={'FOO': 'BAR'}).kwargs,
            {'FOO': 'BAR'},
        )
        self.assertDictEqual(
            x.replace(options={'task_id': '123'}).options,
            {'task_id': '123'},
        )

    def test_set(self):
        self.assertDictEqual(
            Signature('TASK', x=1).set(task_id='2').options,
            {'x': 1, 'task_id': '2'},
        )

    def test_link(self):
        x = signature(SIG)
        x.link(SIG)
        x.link(SIG)
        self.assertIn(SIG, x.options['link'])
        self.assertEqual(len(x.options['link']), 1)

    def test_link_error(self):
        x = signature(SIG)
        x.link_error(SIG)
        x.link_error(SIG)
        self.assertIn(SIG, x.options['link_error'])
        self.assertEqual(len(x.options['link_error']), 1)

    def test_flatten_links(self):
        tasks = [self.add.s(2, 2), self.mul.s(4), self.div.s(2)]
        tasks[0].link(tasks[1])
        tasks[1].link(tasks[2])
        self.assertEqual(tasks[0].flatten_links(), tasks)

    def test_OR(self):
        x = self.add.s(2, 2) | self.mul.s(4)
        self.assertIsInstance(x, chain)
        y = self.add.s(4, 4) | self.div.s(2)
        z = x | y
        self.assertIsInstance(y, chain)
        self.assertIsInstance(z, chain)
        self.assertEqual(len(z.tasks), 4)
        with self.assertRaises(TypeError):
            x | 10
        ax = self.add.s(2, 2) | (self.add.s(4) | self.add.s(8))
        self.assertIsInstance(ax, chain)
        self.assertEqual(len(ax.tasks), 3, 'consolidates chain to chain')

    def test_INVERT(self):
        x = self.add.s(2, 2)
        x.apply_async = Mock()
        x.apply_async.return_value = Mock()
        x.apply_async.return_value.get = Mock()
        x.apply_async.return_value.get.return_value = 4
        self.assertEqual(~x, 4)
        x.apply_async.assert_called()

    def test_merge_immutable(self):
        x = self.add.si(2, 2, foo=1)
        args, kwargs, options = x._merge((4,), {'bar': 2}, {'task_id': 3})
        self.assertTupleEqual(args, (2, 2))
        self.assertDictEqual(kwargs, {'foo': 1})
        self.assertDictEqual(options, {'task_id': 3})

    def test_set_immutable(self):
        x = self.add.s(2, 2)
        self.assertFalse(x.immutable)
        x.set(immutable=True)
        self.assertTrue(x.immutable)
        x.set(immutable=False)
        self.assertFalse(x.immutable)

    def test_election(self):
        x = self.add.s(2, 2)
        x.freeze('foo')
        x.type.app.control = Mock()
        r = x.election()
        x.type.app.control.election.assert_called()
        self.assertEqual(r.id, 'foo')

    def test_AsyncResult_when_not_registered(self):
        s = signature('xxx.not.registered', app=self.app)
        self.assertTrue(s.AsyncResult)

    def test_apply_async_when_not_registered(self):
        s = signature('xxx.not.registered', app=self.app)
        self.assertTrue(s._apply_async)


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

            self.assertEqual(type.from_dict(dict(s)), s)
            self.assertTrue(repr(s))


class test_chunks(CanvasCase):

    def test_chunks(self):
        x = self.add.chunks(range(100), 10)
        self.assertEqual(
            dict(chunks.from_dict(dict(x), app=self.app)), dict(x),
        )

        self.assertTrue(x.group())
        self.assertEqual(len(x.group().tasks), 10)

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

    def test_repr(self):
        x = self.add.s(2, 2) | self.add.s(2)
        self.assertEqual(
            repr(x), '%s(2, 2) | %s(2)' % (self.add.name, self.add.name),
        )

    def test_apply_async(self):
        c = self.add.s(2, 2) | self.add.s(4) | self.add.s(8)
        result = c.apply_async()
        self.assertTrue(result.parent)
        self.assertTrue(result.parent.parent)
        self.assertIsNone(result.parent.parent.parent)

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
        self.assertEqual(tasks[-1].parent_id, 'foo')
        self.assertEqual(tasks[-1].root_id, 'root')
        self.assertEqual(tasks[-2].parent_id, tasks[-1].id)
        self.assertEqual(tasks[-2].root_id, 'root')
        self.assertEqual(tasks[-2].body.parent_id, tasks[-2].tasks.id)
        self.assertEqual(tasks[-2].body.parent_id, tasks[-2].id)
        self.assertEqual(tasks[-2].body.root_id, 'root')
        self.assertEqual(tasks[-2].tasks.tasks[0].parent_id, tasks[-1].id)
        self.assertEqual(tasks[-2].tasks.tasks[0].root_id, 'root')
        self.assertEqual(tasks[-2].tasks.tasks[1].parent_id, tasks[-1].id)
        self.assertEqual(tasks[-2].tasks.tasks[1].root_id, 'root')
        self.assertEqual(tasks[-2].tasks.tasks[2].parent_id, tasks[-1].id)
        self.assertEqual(tasks[-2].tasks.tasks[2].root_id, 'root')
        self.assertEqual(tasks[-2].tasks.tasks[3].parent_id, tasks[-1].id)
        self.assertEqual(tasks[-2].tasks.tasks[3].root_id, 'root')
        self.assertEqual(tasks[-2].tasks.tasks[4].parent_id, tasks[-1].id)
        self.assertEqual(tasks[-2].tasks.tasks[4].root_id, 'root')
        self.assertEqual(tasks[-3].parent_id, tasks[-2].body.id)
        self.assertEqual(tasks[-3].root_id, 'root')
        self.assertEqual(tasks[-4].parent_id, tasks[-3].id)
        self.assertEqual(tasks[-4].root_id, 'root')

    def test_splices_chains(self):
        c = chain(
            self.add.s(5, 5),
            chain(self.add.s(6), self.add.s(7), self.add.s(8), app=self.app),
            app=self.app,
        )
        c.freeze()
        tasks, _ = c._frozen
        self.assertEqual(len(tasks), 4)

    def test_from_dict_no_tasks(self):
        self.assertTrue(chain.from_dict(
            dict(chain(app=self.app)), app=self.app))

    @depends_on_current_app
    def test_app_falls_back_to_default(self):
        from celery._state import current_app
        self.assertIs(chain().app, current_app)

    def test_handles_dicts(self):
        c = chain(
            self.add.s(5, 5), dict(self.add.s(8)), app=self.app,
        )
        c.freeze()
        tasks, _ = c._frozen
        for task in tasks:
            self.assertIsInstance(task, Signature)
            self.assertIs(task.app, self.app)

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

        self.assertEqual(tasks[-1].args[0], 5)
        self.assertIsInstance(tasks[-2], chord)
        self.assertEqual(len(tasks[-2].tasks), 5)
        self.assertEqual(tasks[-2].parent_id, tasks[-1].id)
        self.assertEqual(tasks[-2].root_id, tasks[-1].id)
        self.assertEqual(tasks[-2].body.args[0], 10)
        self.assertEqual(tasks[-2].body.parent_id, tasks[-2].id)

        self.assertEqual(tasks[-3].args[0], 20)
        self.assertEqual(tasks[-3].root_id, tasks[-1].id)
        self.assertEqual(tasks[-3].parent_id, tasks[-2].body.id)

        self.assertEqual(tasks[-4].args[0], 30)
        self.assertEqual(tasks[-4].parent_id, tasks[-3].id)
        self.assertEqual(tasks[-4].root_id, tasks[-1].id)

        self.assertTrue(tasks[-2].body.options['link'])
        self.assertTrue(tasks[-2].body.options['link'][0].options['link'])

        c2 = self.add.s(2, 2) | group(self.add.s(i, i) for i in range(10))
        c2._use_link = True
        tasks2, _ = c2.prepare_steps((), c2.tasks)
        self.assertIsInstance(tasks2[0], group)

    def test_group_to_chord__protocol_2(self):
        c = (
            group([self.add.s(i, i) for i in range(5)], app=self.app) |
            self.add.s(10) |
            self.add.s(20) |
            self.add.s(30)
        )
        c._use_link = False
        tasks, _ = c.prepare_steps((), c.tasks)
        self.assertIsInstance(tasks[-1], chord)

        c2 = self.add.s(2, 2) | group(self.add.s(i, i) for i in range(10))
        c2._use_link = False
        tasks2, _ = c2.prepare_steps((), c2.tasks)
        self.assertIsInstance(tasks2[0], group)

    def test_apply_options(self):

        class static(Signature):

            def clone(self, *args, **kwargs):
                return self

        def s(*args, **kwargs):
            return static(self.add, args, kwargs, type=self.add, app=self.app)

        c = s(2, 2) | s(4) | s(8)
        r1 = c.apply_async(task_id='some_id')
        self.assertEqual(r1.id, 'some_id')

        c.apply_async(group_id='some_group_id')
        self.assertEqual(c.tasks[-1].options['group_id'], 'some_group_id')

        c.apply_async(chord='some_chord_id')
        self.assertEqual(c.tasks[-1].options['chord'], 'some_chord_id')

        c.apply_async(link=[s(32)])
        self.assertListEqual(c.tasks[-1].options['link'], [s(32)])

        c.apply_async(link_error=[s('error')])
        for task in c.tasks:
            self.assertListEqual(task.options['link_error'], [s('error')])

    def test_reverse(self):
        x = self.add.s(2, 2) | self.add.s(2)
        self.assertIsInstance(signature(x), chain)
        self.assertIsInstance(signature(dict(x)), chain)

    def test_always_eager(self):
        self.app.conf.task_always_eager = True
        self.assertEqual(~(self.add.s(4, 4) | self.add.s(8)), 16)

    def test_apply(self):
        x = chain(self.add.s(4, 4), self.add.s(8), self.add.s(10))
        res = x.apply()
        self.assertIsInstance(res, EagerResult)
        self.assertEqual(res.get(), 26)

        self.assertEqual(res.parent.get(), 16)
        self.assertEqual(res.parent.parent.get(), 8)
        self.assertIsNone(res.parent.parent.parent)

    def test_empty_chain_returns_none(self):
        self.assertIsNone(chain(app=self.app)())
        self.assertIsNone(chain(app=self.app).apply_async())

    def test_root_id_parent_id(self):
        self.app.conf.task_protocol = 2
        c = chain(self.add.si(i, i) for i in range(4))
        c.freeze()
        tasks, _ = c._frozen
        for i, task in enumerate(tasks):
            self.assertEqual(task.root_id, tasks[-1].id)
            try:
                self.assertEqual(task.parent_id, tasks[i + 1].id)
            except IndexError:
                assert i == len(tasks) - 1
            else:
                valid_parents = i
        self.assertEqual(valid_parents, len(tasks) - 2)

        self.assert_sent_with_ids(tasks[-1], tasks[-1].id, 'foo',
                                  parent_id='foo')
        self.assertTrue(tasks[-2].options['parent_id'])
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
        self.assertEqual(message.headers['parent_id'], pid)
        self.assertEqual(message.headers['root_id'], rid)

    def test_call_no_tasks(self):
        x = chain()
        self.assertFalse(x())

    def test_call_with_tasks(self):
        x = self.add.s(2, 2) | self.add.s(4)
        x.apply_async = Mock()
        x(2, 2, foo=1)
        x.apply_async.assert_called_with((2, 2), {'foo': 1})

    def test_from_dict_no_args__with_args(self):
        x = dict(self.add.s(2, 2) | self.add.s(4))
        x['args'] = None
        self.assertIsInstance(chain.from_dict(x), chain)
        x['args'] = (2,)
        self.assertIsInstance(chain.from_dict(x), chain)

    def test_accepts_generator_argument(self):
        x = chain(self.add.s(i) for i in range(10))
        self.assertTrue(x.tasks[0].type, self.add)
        self.assertTrue(x.type)


class test_group(CanvasCase):

    def test_repr(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        self.assertTrue(repr(x))

    def test_reverse(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        self.assertIsInstance(signature(x), group)
        self.assertIsInstance(signature(dict(x)), group)

    def test_group_with_group_argument(self):
        g1 = group(self.add.s(2, 2), self.add.s(4, 4), app=self.app)
        g2 = group(g1, app=self.app)
        self.assertIs(g2.tasks, g1.tasks)

    def test_maybe_group_sig(self):
        self.assertListEqual(
            _maybe_group(self.add.s(2, 2), self.app), [self.add.s(2, 2)],
        )

    def test_apply(self):
        x = group([self.add.s(4, 4), self.add.s(8, 8)])
        res = x.apply()
        self.assertEqual(res.get(), [8, 16])

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
        self.assertTrue(res)
        self.assertFalse(res.results)

    def test_apply_async_with_parent(self):
        _task_stack.push(self.add)
        try:
            self.add.push_request(called_directly=False)
            try:
                assert not self.add.request.children
                x = group([self.add.s(4, 4), self.add.s(8, 8)])
                res = x()
                self.assertTrue(self.add.request.children)
                self.assertIn(res, self.add.request.children)
                self.assertEqual(len(self.add.request.children), 1)
            finally:
                self.add.pop_request()
        finally:
            _task_stack.pop()

    def test_from_dict(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        x['args'] = (2, 2)
        self.assertTrue(group.from_dict(dict(x)))
        x['args'] = None
        self.assertTrue(group.from_dict(dict(x)))

    def test_call_empty_group(self):
        x = group(app=self.app)
        self.assertFalse(len(x()))
        x.delay()
        x.apply_async()
        x()

    def test_skew(self):
        g = group([self.add.s(i, i) for i in range(10)])
        g.skew(start=1, stop=10, step=1)
        for i, task in enumerate(g.tasks):
            self.assertEqual(task.options['countdown'], i + 1)

    def test_iter(self):
        g = group([self.add.s(i, i) for i in range(10)])
        self.assertListEqual(list(iter(g)), g.tasks)


class test_chord(CanvasCase):

    def test_reverse(self):
        x = chord([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        self.assertIsInstance(signature(x), chord)
        self.assertIsInstance(signature(dict(x)), chord)

    def test_clone_clones_body(self):
        x = chord([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        y = x.clone()
        self.assertIsNot(x.kwargs['body'], y.kwargs['body'])
        y.kwargs.pop('body')
        z = y.clone()
        self.assertIsNone(z.kwargs.get('body'))

    def test_argument_is_group(self):
        x = chord(group(self.add.s(2, 2), self.add.s(4, 4), app=self.app))
        self.assertTrue(x.tasks)

    def test_set_parent_id(self):
        x = chord(group(self.add.s(2, 2)))
        x.tasks = [self.add.s(2, 2)]
        x.set_parent_id('pid')

    def test_app_when_app(self):
        app = Mock(name='app')
        x = chord([self.add.s(4, 4)], app=app)
        self.assertIs(x.app, app)

    def test_app_when_app_in_task(self):
        t1 = Mock(name='t1')
        t2 = Mock(name='t2')
        x = chord([t1, self.add.s(4, 4)])
        self.assertIs(x.app, x.tasks[0].app)
        t1.app = None
        x = chord([t1], body=t2)
        self.assertIs(x.app, t2._app)

    @depends_on_current_app
    def test_app_fallback_to_current(self):
        from celery._state import current_app
        t1 = Mock(name='t1')
        t1.app = t1._app = None
        x = chord([t1], body=t1)
        self.assertIs(x.app, current_app)

    def test_set_immutable(self):
        x = chord([Mock(name='t1'), Mock(name='t2')], app=self.app)
        x.set_immutable(True)

    def test_links_to_body(self):
        x = chord([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        x.link(self.div.s(2))
        self.assertFalse(x.options.get('link'))
        self.assertTrue(x.kwargs['body'].options['link'])

        x.link_error(self.div.s(2))
        self.assertFalse(x.options.get('link_error'))
        self.assertTrue(x.kwargs['body'].options['link_error'])

        self.assertTrue(x.tasks)
        self.assertTrue(x.body)

    def test_repr(self):
        x = chord([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        self.assertTrue(repr(x))
        x.kwargs['body'] = None
        self.assertIn('without body', repr(x))

    def test_freeze_tasks_is_not_group(self):
        x = chord([self.add.s(2, 2)], body=self.add.s(), app=self.app)
        x.freeze()
        x.tasks = [self.add.s(2, 2)]
        x.freeze()


class test_maybe_signature(CanvasCase):

    def test_is_None(self):
        self.assertIsNone(maybe_signature(None, app=self.app))

    def test_is_dict(self):
        self.assertIsInstance(
            maybe_signature(dict(self.add.s()), app=self.app), Signature,
        )

    def test_when_sig(self):
        s = self.add.s()
        self.assertIs(maybe_signature(s, app=self.app), s)
