from __future__ import absolute_import

from mock import Mock

from celery import shared_task
from celery.canvas import (
    Signature,
    chain,
    group,
    chord,
    subtask,
    xmap,
    xstarmap,
    chunks,
    _maybe_group,
    maybe_subtask,
)
from celery.result import EagerResult

from celery.tests.case import AppCase

SIG = Signature({'task': 'TASK',
                 'args': ('A1', ),
                 'kwargs': {'K1': 'V1'},
                 'options': {'task_id': 'TASK_ID'},
                 'subtask_type': ''})


@shared_task()
def add(x, y):
    return x + y


@shared_task()
def mul(x, y):
    return x * y


@shared_task()
def div(x, y):
    return x / y


class test_Signature(AppCase):

    def test_getitem_property_class(self):
        self.assertTrue(Signature.task)
        self.assertTrue(Signature.args)
        self.assertTrue(Signature.kwargs)
        self.assertTrue(Signature.options)
        self.assertTrue(Signature.subtask_type)

    def test_getitem_property(self):
        self.assertEqual(SIG.task, 'TASK')
        self.assertEqual(SIG.args, ('A1', ))
        self.assertEqual(SIG.kwargs, {'K1': 'V1'})
        self.assertEqual(SIG.options, {'task_id': 'TASK_ID'})
        self.assertEqual(SIG.subtask_type, '')

    def test_replace(self):
        x = Signature('TASK', ('A'), {})
        self.assertTupleEqual(x.replace(args=('B', )).args, ('B', ))
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
        x = subtask(SIG)
        x.link(SIG)
        x.link(SIG)
        self.assertIn(SIG, x.options['link'])
        self.assertEqual(len(x.options['link']), 1)

    def test_link_error(self):
        x = subtask(SIG)
        x.link_error(SIG)
        x.link_error(SIG)
        self.assertIn(SIG, x.options['link_error'])
        self.assertEqual(len(x.options['link_error']), 1)

    def test_flatten_links(self):
        tasks = [add.s(2, 2), mul.s(4), div.s(2)]
        tasks[0].link(tasks[1])
        tasks[1].link(tasks[2])
        self.assertEqual(tasks[0].flatten_links(), tasks)

    def test_OR(self):
        x = add.s(2, 2) | mul.s(4)
        self.assertIsInstance(x, chain)
        y = add.s(4, 4) | div.s(2)
        z = x | y
        self.assertIsInstance(y, chain)
        self.assertIsInstance(z, chain)
        self.assertEqual(len(z.tasks), 4)
        with self.assertRaises(TypeError):
            x | 10
        ax = add.s(2, 2) | (add.s(4) | add.s(8))
        self.assertIsInstance(ax, chain)
        self.assertEqual(len(ax.tasks), 3, 'consolidates chain to chain')

    def test_INVERT(self):
        x = add.s(2, 2)
        x.apply_async = Mock()
        x.apply_async.return_value = Mock()
        x.apply_async.return_value.get = Mock()
        x.apply_async.return_value.get.return_value = 4
        self.assertEqual(~x, 4)
        self.assertTrue(x.apply_async.called)

    def test_merge_immutable(self):
        x = add.si(2, 2, foo=1)
        args, kwargs, options = x._merge((4, ), {'bar': 2}, {'task_id': 3})
        self.assertTupleEqual(args, (2, 2))
        self.assertDictEqual(kwargs, {'foo': 1})
        self.assertDictEqual(options, {'task_id': 3})

    def test_set_immutable(self):
        x = add.s(2, 2)
        self.assertFalse(x.immutable)
        x.set(immutable=True)
        self.assertTrue(x.immutable)
        x.set(immutable=False)
        self.assertFalse(x.immutable)

    def test_election(self):
        x = add.s(2, 2)
        x.freeze('foo')
        prev, x.type.app.control = x.type.app.control, Mock()
        try:
            r = x.election()
            self.assertTrue(x.type.app.control.election.called)
            self.assertEqual(r.id, 'foo')
        finally:
            x.type.app.control = prev

    def test_AsyncResult_when_not_registerd(self):
        s = subtask('xxx.not.registered')
        self.assertTrue(s.AsyncResult)

    def test_apply_async_when_not_registered(self):
        s = subtask('xxx.not.registered')
        self.assertTrue(s._apply_async)


class test_xmap_xstarmap(AppCase):

    def test_apply(self):
        for type, attr in [(xmap, 'map'), (xstarmap, 'starmap')]:
            args = [(i, i) for i in range(10)]
            s = getattr(add, attr)(args)
            s.type = Mock()

            s.apply_async(foo=1)
            s.type.apply_async.assert_called_with(
                (), {'task': add.s(), 'it': args}, foo=1,
            )

            self.assertEqual(type.from_dict(dict(s)), s)
            self.assertTrue(repr(s))


class test_chunks(AppCase):

    def test_chunks(self):
        x = add.chunks(range(100), 10)
        self.assertEqual(chunks.from_dict(dict(x)), x)

        self.assertTrue(x.group())
        self.assertEqual(len(x.group().tasks), 10)

        x.group = Mock()
        gr = x.group.return_value = Mock()

        x.apply_async()
        gr.apply_async.assert_called_with((), {})

        x()
        gr.assert_called_with()

        self.app.conf.CELERY_ALWAYS_EAGER = True
        try:
            chunks.apply_chunks(**x['kwargs'])
        finally:
            self.app.conf.CELERY_ALWAYS_EAGER = False


class test_chain(AppCase):

    def test_repr(self):
        x = add.s(2, 2) | add.s(2)
        self.assertEqual(repr(x), '%s(2, 2) | %s(2)' % (add.name, add.name))

    def test_reverse(self):
        x = add.s(2, 2) | add.s(2)
        self.assertIsInstance(subtask(x), chain)
        self.assertIsInstance(subtask(dict(x)), chain)

    def test_always_eager(self):
        self.app.conf.CELERY_ALWAYS_EAGER = True
        try:
            self.assertEqual(~(add.s(4, 4) | add.s(8)), 16)
        finally:
            self.app.conf.CELERY_ALWAYS_EAGER = False

    def test_apply(self):
        x = chain(add.s(4, 4), add.s(8), add.s(10))
        res = x.apply()
        self.assertIsInstance(res, EagerResult)
        self.assertEqual(res.get(), 26)

        self.assertEqual(res.parent.get(), 16)
        self.assertEqual(res.parent.parent.get(), 8)
        self.assertIsNone(res.parent.parent.parent)

    def test_call_no_tasks(self):
        x = chain()
        self.assertFalse(x())

    def test_call_with_tasks(self):
        x = add.s(2, 2) | add.s(4)
        x.apply_async = Mock()
        x(2, 2, foo=1)
        x.apply_async.assert_called_with((2, 2), {'foo': 1})

    def test_from_dict_no_args__with_args(self):
        x = dict(add.s(2, 2) | add.s(4))
        x['args'] = None
        self.assertIsInstance(chain.from_dict(x), chain)
        x['args'] = (2, )
        self.assertIsInstance(chain.from_dict(x), chain)

    def test_accepts_generator_argument(self):
        x = chain(add.s(i) for i in range(10))
        self.assertTrue(x.tasks[0].type, add)
        self.assertTrue(x.type)


class test_group(AppCase):

    def test_repr(self):
        x = group([add.s(2, 2), add.s(4, 4)])
        self.assertEqual(repr(x), repr(x.tasks))

    def test_reverse(self):
        x = group([add.s(2, 2), add.s(4, 4)])
        self.assertIsInstance(subtask(x), group)
        self.assertIsInstance(subtask(dict(x)), group)

    def test_maybe_group_sig(self):
        self.assertListEqual(_maybe_group(add.s(2, 2)), [add.s(2, 2)])

    def test_from_dict(self):
        x = group([add.s(2, 2), add.s(4, 4)])
        x['args'] = (2, 2)
        self.assertTrue(group.from_dict(dict(x)))
        x['args'] = None
        self.assertTrue(group.from_dict(dict(x)))

    def test_call_empty_group(self):
        x = group()
        self.assertIsNone(x())

    def test_skew(self):
        g = group([add.s(i, i) for i in range(10)])
        g.skew(start=1, stop=10, step=1)
        for i, task in enumerate(g.tasks):
            self.assertEqual(task.options['countdown'], i + 1)

    def test_iter(self):
        g = group([add.s(i, i) for i in range(10)])
        self.assertListEqual(list(iter(g)), g.tasks)


class test_chord(AppCase):

    def test_reverse(self):
        x = chord([add.s(2, 2), add.s(4, 4)], body=mul.s(4))
        self.assertIsInstance(subtask(x), chord)
        self.assertIsInstance(subtask(dict(x)), chord)

    def test_clone_clones_body(self):
        x = chord([add.s(2, 2), add.s(4, 4)], body=mul.s(4))
        y = x.clone()
        self.assertIsNot(x.kwargs['body'], y.kwargs['body'])
        y.kwargs.pop('body')
        z = y.clone()
        self.assertIsNone(z.kwargs.get('body'))

    def test_links_to_body(self):
        x = chord([add.s(2, 2), add.s(4, 4)], body=mul.s(4))
        x.link(div.s(2))
        self.assertFalse(x.options.get('link'))
        self.assertTrue(x.kwargs['body'].options['link'])

        x.link_error(div.s(2))
        self.assertFalse(x.options.get('link_error'))
        self.assertTrue(x.kwargs['body'].options['link_error'])

        self.assertTrue(x.tasks)
        self.assertTrue(x.body)

    def test_repr(self):
        x = chord([add.s(2, 2), add.s(4, 4)], body=mul.s(4))
        self.assertTrue(repr(x))
        x.kwargs['body'] = None
        self.assertIn('without body', repr(x))


class test_maybe_subtask(AppCase):

    def test_is_None(self):
        self.assertIsNone(maybe_subtask(None))

    def test_is_dict(self):
        self.assertIsInstance(maybe_subtask(dict(add.s())), Signature)

    def test_when_sig(self):
        s = add.s()
        self.assertIs(maybe_subtask(s), s)
