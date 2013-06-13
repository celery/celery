from __future__ import absolute_import

from mock import Mock, patch

from celery import group, shared_task, chord
from celery.app import builtins
from celery.canvas import Signature
from celery.five import range
from celery._state import _task_stack
from celery.tests.case import AppCase


@shared_task()
def add(x, y):
    return x + y


@shared_task()
def xsum(x):
    return sum(x)


class test_backend_cleanup(AppCase):

    def test_run(self):
        prev = self.app.backend
        self.app.backend.cleanup = Mock()
        self.app.backend.cleanup.__name__ = 'cleanup'
        try:
            cleanup_task = builtins.add_backend_cleanup_task(self.app)
            cleanup_task()
            self.assertTrue(self.app.backend.cleanup.called)
        finally:
            self.app.backend = prev


class test_map(AppCase):

    def test_run(self):

        @self.app.task()
        def map_mul(x):
            return x[0] * x[1]

        res = self.app.tasks['celery.map'](
            map_mul, [(2, 2), (4, 4), (8, 8)],
        )
        self.assertEqual(res, [4, 16, 64])


class test_starmap(AppCase):

    def test_run(self):

        @self.app.task()
        def smap_mul(x, y):
            return x * y

        res = self.app.tasks['celery.starmap'](
            smap_mul, [(2, 2), (4, 4), (8, 8)],
        )
        self.assertEqual(res, [4, 16, 64])


class test_chunks(AppCase):

    @patch('celery.canvas.chunks.apply_chunks')
    def test_run(self, apply_chunks):

        @self.app.task()
        def chunks_mul(l):
            return l

        self.app.tasks['celery.chunks'](
            chunks_mul, [(2, 2), (4, 4), (8, 8)], 1,
        )
        self.assertTrue(apply_chunks.called)


class test_group(AppCase):

    def setup(self):
        self.prev = self.app.tasks.get('celery.group')
        self.task = builtins.add_group_task(self.app)()

    def teardown(self):
        self.app.tasks['celery.group'] = self.prev

    def test_apply_async_eager(self):
        self.task.apply = Mock()
        self.app.conf.CELERY_ALWAYS_EAGER = True
        try:
            self.task.apply_async()
        finally:
            self.app.conf.CELERY_ALWAYS_EAGER = False
        self.assertTrue(self.task.apply.called)

    def test_apply(self):
        x = group([add.s(4, 4), add.s(8, 8)])
        x.name = self.task.name
        res = x.apply()
        self.assertEqual(res.get(), [8, 16])

    def test_apply_async(self):
        x = group([add.s(4, 4), add.s(8, 8)])
        x.apply_async()

    def test_apply_empty(self):
        x = group()
        x.apply()
        res = x.apply_async()
        self.assertFalse(res)
        self.assertFalse(res.results)

    def test_apply_async_with_parent(self):
        _task_stack.push(add)
        try:
            add.push_request(called_directly=False)
            try:
                assert not add.request.children
                x = group([add.s(4, 4), add.s(8, 8)])
                res = x()
                self.assertTrue(add.request.children)
                self.assertIn(res, add.request.children)
                self.assertEqual(len(add.request.children), 1)
            finally:
                add.pop_request()
        finally:
            _task_stack.pop()


class test_chain(AppCase):

    def setup(self):
        self.prev = self.app.tasks.get('celery.chain')
        self.task = builtins.add_chain_task(self.app)()

    def teardown(self):
        self.app.tasks['celery.chain'] = self.prev

    def test_apply_async(self):
        c = add.s(2, 2) | add.s(4) | add.s(8)
        result = c.apply_async()
        self.assertTrue(result.parent)
        self.assertTrue(result.parent.parent)
        self.assertIsNone(result.parent.parent.parent)

    def test_group_to_chord(self):
        c = (
            group(add.s(i, i) for i in range(5)) |
            add.s(10) |
            add.s(20) |
            add.s(30)
        )
        tasks, _ = c.type.prepare_steps((), c.tasks)
        self.assertIsInstance(tasks[0], chord)
        self.assertTrue(tasks[0].body.options['link'])
        self.assertTrue(tasks[0].body.options['link'][0].options['link'])

        c2 = add.s(2, 2) | group(add.s(i, i) for i in range(10))
        tasks2, _ = c2.type.prepare_steps((), c2.tasks)
        self.assertIsInstance(tasks2[1], group)

    def test_apply_options(self):

        class static(Signature):

            def clone(self, *args, **kwargs):
                return self

        def s(*args, **kwargs):
            return static(add.name, args, kwargs)

        c = s(2, 2) | s(4, 4) | s(8, 8)
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


class test_chord(AppCase):

    def setup(self):
        self.prev = self.app.tasks.get('celery.chord')
        self.task = builtins.add_chord_task(self.app)()

    def teardown(self):
        self.app.tasks['celery.chord'] = self.prev

    def test_apply_async(self):
        x = chord([add.s(i, i) for i in range(10)], body=xsum.s())
        r = x.apply_async()
        self.assertTrue(r)
        self.assertTrue(r.parent)

    def test_run_header_not_group(self):
        self.task([add.s(i, i) for i in range(10)], xsum.s())

    def test_forward_options(self):
        body = xsum.s()
        x = chord([add.s(i, i) for i in range(10)], body=body)
        x.apply_async(group_id='some_group_id')
        self.assertEqual(body.options['group_id'], 'some_group_id')
        x2 = chord([add.s(i, i) for i in range(10)], body=body)
        x2.apply_async(chord='some_chord_id')
        self.assertEqual(body.options['chord'], 'some_chord_id')

    def test_apply_eager(self):
        self.app.conf.CELERY_ALWAYS_EAGER = True
        try:
            x = chord([add.s(i, i) for i in range(10)], body=xsum.s())
            r = x.apply_async()
            self.assertEqual(r.get(), 90)
        finally:
            self.app.conf.CELERY_ALWAYS_EAGER = False
