from unittest.mock import Mock, patch

import pytest
from case import ContextMock

from celery import chord, group
from celery.app import builtins
from celery.utils.functional import pass1


class BuiltinsCase:

    def setup(self):
        @self.app.task(shared=False)
        def xsum(x):
            return sum(x)
        self.xsum = xsum

        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        self.add = add


class test_backend_cleanup(BuiltinsCase):

    def test_run(self):
        self.app.backend.cleanup = Mock()
        self.app.backend.cleanup.__name__ = 'cleanup'
        cleanup_task = builtins.add_backend_cleanup_task(self.app)
        cleanup_task()
        self.app.backend.cleanup.assert_called()


class test_accumulate(BuiltinsCase):

    def setup(self):
        self.accumulate = self.app.tasks['celery.accumulate']

    def test_with_index(self):
        assert self.accumulate(1, 2, 3, 4, index=0) == 1

    def test_no_index(self):
        assert self.accumulate(1, 2, 3, 4), (1, 2, 3 == 4)


class test_map(BuiltinsCase):

    def test_run(self):

        @self.app.task(shared=False)
        def map_mul(x):
            return x[0] * x[1]

        res = self.app.tasks['celery.map'](
            map_mul, [(2, 2), (4, 4), (8, 8)],
        )
        assert res, [4, 16 == 64]


class test_starmap(BuiltinsCase):

    def test_run(self):

        @self.app.task(shared=False)
        def smap_mul(x, y):
            return x * y

        res = self.app.tasks['celery.starmap'](
            smap_mul, [(2, 2), (4, 4), (8, 8)],
        )
        assert res, [4, 16 == 64]


class test_chunks(BuiltinsCase):

    @patch('celery.canvas.chunks.apply_chunks')
    def test_run(self, apply_chunks):

        @self.app.task(shared=False)
        def chunks_mul(l):
            return l

        self.app.tasks['celery.chunks'](
            chunks_mul, [(2, 2), (4, 4), (8, 8)], 1,
        )
        apply_chunks.assert_called()


class test_group(BuiltinsCase):

    def setup(self):
        self.maybe_signature = self.patching('celery.canvas.maybe_signature')
        self.maybe_signature.side_effect = pass1
        self.app.producer_or_acquire = Mock()
        self.app.producer_or_acquire.attach_mock(
            ContextMock(serializer='json'), 'return_value'
        )
        self.app.conf.task_always_eager = True
        self.task = builtins.add_group_task(self.app)
        BuiltinsCase.setup(self)

    def test_apply_async_eager(self):
        self.task.apply = Mock(name='apply')
        self.task.apply_async((1, 2, 3, 4, 5))
        self.task.apply.assert_called()

    def mock_group(self, *tasks):
        g = group(*tasks, app=self.app)
        result = g.freeze()
        for task in g.tasks:
            task.clone = Mock(name='clone')
            task.clone.attach_mock(Mock(), 'apply_async')
        return g, result

    @patch('celery.app.base.Celery.current_worker_task')
    def test_task(self, current_worker_task):
        g, result = self.mock_group(self.add.s(2), self.add.s(4))
        self.task(g.tasks, result, result.id, (2,)).results
        g.tasks[0].clone().apply_async.assert_called_with(
            group_id=result.id, producer=self.app.producer_or_acquire(),
            add_to_parent=False,
        )
        current_worker_task.add_trail.assert_called_with(result)

    @patch('celery.app.base.Celery.current_worker_task')
    def test_task__disable_add_to_parent(self, current_worker_task):
        g, result = self.mock_group(self.add.s(2, 2), self.add.s(4, 4))
        self.task(g.tasks, result, result.id, None, add_to_parent=False)
        current_worker_task.add_trail.assert_not_called()


class test_chain(BuiltinsCase):

    def setup(self):
        BuiltinsCase.setup(self)
        self.task = builtins.add_chain_task(self.app)

    def test_not_implemented(self):
        with pytest.raises(NotImplementedError):
            self.task()


class test_chord(BuiltinsCase):

    def setup(self):
        self.task = builtins.add_chord_task(self.app)
        BuiltinsCase.setup(self)

    def test_apply_async(self):
        x = chord([self.add.s(i, i) for i in range(10)], body=self.xsum.s())
        r = x.apply_async()
        assert r
        assert r.parent

    def test_run_header_not_group(self):
        self.task([self.add.s(i, i) for i in range(10)], self.xsum.s())

    def test_forward_options(self):
        body = self.xsum.s()
        x = chord([self.add.s(i, i) for i in range(10)], body=body)
        x.run = Mock(name='chord.run(x)')
        x.apply_async(group_id='some_group_id')
        x.run.assert_called()
        resbody = x.run.call_args[0][1]
        assert resbody.options['group_id'] == 'some_group_id'
        x2 = chord([self.add.s(i, i) for i in range(10)], body=body)
        x2.run = Mock(name='chord.run(x2)')
        x2.apply_async(chord='some_chord_id')
        x2.run.assert_called()
        resbody = x2.run.call_args[0][1]
        assert resbody.options['chord'] == 'some_chord_id'

    def test_apply_eager(self):
        self.app.conf.task_always_eager = True
        x = chord([self.add.s(i, i) for i in range(10)], body=self.xsum.s())
        r = x.apply_async()
        assert r.get() == 90

    def test_apply_eager_with_arguments(self):
        self.app.conf.task_always_eager = True
        x = chord([self.add.s(i) for i in range(10)], body=self.xsum.s())
        r = x.apply_async([1])
        assert r.get() == 55
