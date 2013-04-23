from __future__ import absolute_import
from __future__ import with_statement

from mock import Mock

from celery import current_app, task
from celery.canvas import Signature, chain, group, chord, subtask
from celery.result import EagerResult

from celery.tests.utils import Case

SIG = Signature({'task': 'TASK',
                 'args': ('A1', ),
                 'kwargs': {'K1': 'V1'},
                 'options': {'task_id': 'TASK_ID'},
                 'subtask_type': ''})


@task()
def add(x, y):
    return x + y


@task()
def mul(x, y):
    return x * y


@task()
def div(x, y):
    return x / y


class test_Signature(Case):

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

    def test_INVERT(self):
        x = add.s(2, 2)
        x.apply_async = Mock()
        x.apply_async.return_value = Mock()
        x.apply_async.return_value.get = Mock()
        x.apply_async.return_value.get.return_value = 4
        self.assertEqual(~x, 4)
        self.assertTrue(x.apply_async.called)


class test_chain(Case):

    def test_repr(self):
        x = add.s(2, 2) | add.s(2)
        self.assertEqual(repr(x), '%s(2, 2) | %s(2)' % (add.name, add.name))

    def test_reverse(self):
        x = add.s(2, 2) | add.s(2)
        self.assertIsInstance(subtask(x), chain)
        self.assertIsInstance(subtask(dict(x)), chain)

    def test_always_eager(self):
        current_app.conf.CELERY_ALWAYS_EAGER = True
        try:
            self.assertEqual(~(add.s(4, 4) | add.s(8)), 16)
        finally:
            current_app.conf.CELERY_ALWAYS_EAGER = False

    def test_apply(self):
        x = chain(add.s(4, 4), add.s(8), add.s(10))
        res = x.apply()
        self.assertIsInstance(res, EagerResult)
        self.assertEqual(res.get(), 26)

        self.assertEqual(res.parent.get(), 16)
        self.assertEqual(res.parent.parent.get(), 8)
        self.assertIsNone(res.parent.parent.parent)

    def test_accepts_generator_argument(self):
        x = chain(add.s(i) for i in range(10))
        self.assertTrue(x.tasks[0].type, add)
        self.assertTrue(x.type)


class test_group(Case):

    def test_repr(self):
        x = group([add.s(2, 2), add.s(4, 4)])
        self.assertEqual(repr(x), repr(x.tasks))

    def test_reverse(self):
        x = group([add.s(2, 2), add.s(4, 4)])
        self.assertIsInstance(subtask(x), group)
        self.assertIsInstance(subtask(dict(x)), group)


class test_chord(Case):

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
