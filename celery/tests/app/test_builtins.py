from __future__ import absolute_import

from mock import Mock, patch

from celery import current_app as app, group, task, chord
from celery.app import builtins
from celery._state import _task_stack
from celery.tests.utils import Case


@task()
def add(x, y):
    return x + y


@task()
def xsum(x):
    return sum(x)


class test_backend_cleanup(Case):

    def test_run(self):
        prev = app.backend
        app.backend.cleanup = Mock()
        app.backend.cleanup.__name__ = 'cleanup'
        try:
            cleanup_task = builtins.add_backend_cleanup_task(app)
            cleanup_task()
            self.assertTrue(app.backend.cleanup.called)
        finally:
            app.backend = prev


class test_map(Case):

    def test_run(self):

        @app.task()
        def map_mul(x):
            return x[0] * x[1]

        res = app.tasks['celery.map'](map_mul, [(2, 2), (4, 4), (8, 8)])
        self.assertEqual(res, [4, 16, 64])


class test_starmap(Case):

    def test_run(self):

        @app.task()
        def smap_mul(x, y):
            return x * y

        res = app.tasks['celery.starmap'](smap_mul, [(2, 2), (4, 4), (8, 8)])
        self.assertEqual(res, [4, 16, 64])


class test_chunks(Case):

    @patch('celery.canvas.chunks.apply_chunks')
    def test_run(self, apply_chunks):

        @app.task()
        def chunks_mul(l):
            return l

        app.tasks['celery.chunks'](
            chunks_mul, [(2, 2), (4, 4), (8, 8)], 1,
        )
        self.assertTrue(apply_chunks.called)


class test_group(Case):

    def setUp(self):
        self.prev = app.tasks.get('celery.group')
        self.task = builtins.add_group_task(app)()

    def tearDown(self):
        app.tasks['celery.group'] = self.prev

    def test_apply_async_eager(self):
        self.task.apply = Mock()
        app.conf.CELERY_ALWAYS_EAGER = True
        try:
            self.task.apply_async()
        finally:
            app.conf.CELERY_ALWAYS_EAGER = False
        self.assertTrue(self.task.apply.called)

    def test_apply(self):
        x = group([add.s(4, 4), add.s(8, 8)])
        x.name = self.task.name
        res = x.apply()
        self.assertEqual(res.get(), [8, 16])

    def test_apply_async(self):
        x = group([add.s(4, 4), add.s(8, 8)])
        x.apply_async()

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


class test_chain(Case):

    def setUp(self):
        self.prev = app.tasks.get('celery.chain')
        self.task = builtins.add_chain_task(app)()

    def tearDown(self):
        app.tasks['celery.chain'] = self.prev

    def test_apply_async(self):
        c = add.s(2, 2) | add.s(4) | add.s(8)
        result = c.apply_async()
        self.assertTrue(result.parent)
        self.assertTrue(result.parent.parent)
        self.assertIsNone(result.parent.parent.parent)


class test_chord(Case):

    def setUp(self):
        self.prev = app.tasks.get('celery.chord')
        self.task = builtins.add_chord_task(app)()

    def tearDown(self):
        app.tasks['celery.chord'] = self.prev

    def test_apply_async(self):
        x = chord([add.s(i, i) for i in xrange(10)], body=xsum.s())
        r = x.apply_async()
        self.assertTrue(r)
        self.assertTrue(r.parent)

    def test_run_header_not_group(self):
        self.task([add.s(i, i) for i in xrange(10)], xsum.s())

    def test_apply_eager(self):
        app.conf.CELERY_ALWAYS_EAGER = True
        try:
            x = chord([add.s(i, i) for i in xrange(10)], body=xsum.s())
            r = x.apply_async()
            self.assertEqual(r.get(), 90)

        finally:
            app.conf.CELERY_ALWAYS_EAGER = False
