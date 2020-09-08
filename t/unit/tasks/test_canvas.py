import json
from unittest.mock import MagicMock, Mock, call, patch, sentinel

import pytest
import pytest_subtests  # noqa: F401

from celery._state import _task_stack
from celery.canvas import (Signature, _chain, _maybe_group, chain, chord,
                           chunks, group, maybe_signature, maybe_unroll_group,
                           signature, xmap, xstarmap)
from celery.result import AsyncResult, EagerResult, GroupResult

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
        assert isinstance(x, _chain)
        y = self.add.s(4, 4) | self.div.s(2)
        z = x | y
        assert isinstance(y, _chain)
        assert isinstance(z, _chain)
        assert len(z.tasks) == 4
        with pytest.raises(TypeError):
            x | 10
        ax = self.add.s(2, 2) | (self.add.s(4) | self.add.s(8))
        assert isinstance(ax, _chain)
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

    def test_merge_options__none(self):
        sig = self.add.si()
        _, _, new_options = sig._merge()
        assert new_options is sig.options
        _, _, new_options = sig._merge(options=None)
        assert new_options is sig.options

    @pytest.mark.parametrize("immutable_sig", (True, False))
    def test_merge_options__group_id(self, immutable_sig):
        # This is to avoid testing the behaviour in `test_set_immutable()`
        if immutable_sig:
            sig = self.add.si()
        else:
            sig = self.add.s()
        # If the signature has no group ID, it can be set
        assert not sig.options
        _, _, new_options = sig._merge(options={"group_id": sentinel.gid})
        assert new_options == {"group_id": sentinel.gid}
        # But if one is already set, the new one is silently ignored
        sig.set(group_id=sentinel.old_gid)
        _, _, new_options = sig._merge(options={"group_id": sentinel.new_gid})
        assert new_options == {"group_id": sentinel.old_gid}

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

    def test_keeping_link_error_on_chaining(self):
        x = self.add.s(2, 2) | self.mul.s(4)
        assert isinstance(x, _chain)
        x.link_error(SIG)
        assert SIG in x.options['link_error']

        t = signature(SIG)
        z = x | t
        assert isinstance(z, _chain)
        assert t in z.tasks
        assert not z.options.get('link_error')
        assert SIG in z.tasks[0].options['link_error']
        assert not z.tasks[2].options.get('link_error')
        assert SIG in x.options['link_error']
        assert t not in x.tasks
        assert not x.tasks[0].options.get('link_error')

        z = t | x
        assert isinstance(z, _chain)
        assert t in z.tasks
        assert not z.options.get('link_error')
        assert SIG in z.tasks[1].options['link_error']
        assert not z.tasks[0].options.get('link_error')
        assert SIG in x.options['link_error']
        assert t not in x.tasks
        assert not x.tasks[0].options.get('link_error')

        y = self.add.s(4, 4) | self.div.s(2)
        assert isinstance(y, _chain)

        z = x | y
        assert isinstance(z, _chain)
        assert not z.options.get('link_error')
        assert SIG in z.tasks[0].options['link_error']
        assert not z.tasks[2].options.get('link_error')
        assert SIG in x.options['link_error']
        assert not x.tasks[0].options.get('link_error')

        z = y | x
        assert isinstance(z, _chain)
        assert not z.options.get('link_error')
        assert SIG in z.tasks[3].options['link_error']
        assert not z.tasks[1].options.get('link_error')
        assert SIG in x.options['link_error']
        assert not x.tasks[0].options.get('link_error')


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

    def test_chain_of_chain_with_a_single_task(self):
        s = self.add.s(1, 1)
        assert chain([chain(s)]).tasks == list(chain(s).tasks)

    def test_clone_preserves_state(self):
        x = chain(self.add.s(i, i) for i in range(10))
        assert x.clone().tasks == x.tasks
        assert x.clone().kwargs == x.kwargs
        assert x.clone().args == x.args

    def test_repr(self):
        x = self.add.s(2, 2) | self.add.s(2)
        assert repr(x) == f'{self.add.name}(2, 2) | add(2)'

    def test_apply_async(self):
        c = self.add.s(2, 2) | self.add.s(4) | self.add.s(8)
        result = c.apply_async()
        assert result.parent
        assert result.parent.parent
        assert result.parent.parent.parent is None

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

    def test_from_dict_full_subtasks(self):
        c = chain(self.add.si(1, 2), self.add.si(3, 4), self.add.si(5, 6))
        serialized = json.loads(json.dumps(c))
        deserialized = chain.from_dict(serialized)
        assert all(isinstance(task, Signature) for task in deserialized.tasks)

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
        assert all(isinstance(task, Signature) for task in tasks)
        assert all(task.app is self.app for task in tasks)

    def test_groups_in_chain_to_chord(self):
        g1 = group([self.add.s(2, 2), self.add.s(4, 4)])
        g2 = group([self.add.s(3, 3), self.add.s(5, 5)])
        c = g1 | g2
        assert isinstance(c, chord)

    def test_group_to_chord(self):
        c = (
            self.add.s(5) |
            group([self.add.s(i, i) for i in range(5)], app=self.app) |
            self.add.s(10) |
            self.add.s(20) |
            self.add.s(30)
        )
        c._use_link = True
        tasks, results = c.prepare_steps((), {}, c.tasks)

        assert tasks[-1].args[0] == 5
        assert isinstance(tasks[-2], chord)
        assert len(tasks[-2].tasks) == 5

        body = tasks[-2].body
        assert len(body.tasks) == 3
        assert body.tasks[0].args[0] == 10
        assert body.tasks[1].args[0] == 20
        assert body.tasks[2].args[0] == 30

        c2 = self.add.s(2, 2) | group(self.add.s(i, i) for i in range(10))
        c2._use_link = True
        tasks2, _ = c2.prepare_steps((), {}, c2.tasks)
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
        assert isinstance(c, chord)
        assert isinstance(c.body, _chain)
        assert len(c.body.tasks) == 3

        c2 = self.add.s(2, 2) | group(self.add.s(i, i) for i in range(10))
        c2._use_link = False
        tasks2, _ = c2.prepare_steps((), {}, c2.tasks)
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

    def test_apply_options_none(self):
        class static(Signature):

            def clone(self, *args, **kwargs):
                return self

            def _apply_async(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        c = static(self.add, (2, 2), type=self.add, app=self.app, priority=5)

        c.apply_async(priority=4)
        assert c.kwargs['priority'] == 4

        c.apply_async(priority=None)
        assert c.kwargs['priority'] == 5

    def test_reverse(self):
        x = self.add.s(2, 2) | self.add.s(2)
        assert isinstance(signature(x), _chain)
        assert isinstance(signature(dict(x)), _chain)

    def test_always_eager(self):
        self.app.conf.task_always_eager = True
        assert ~(self.add.s(4, 4) | self.add.s(8)) == 16

    def test_chain_always_eager(self):
        self.app.conf.task_always_eager = True
        from celery import _state, result

        fixture_task_join_will_block = _state.task_join_will_block
        try:
            _state.task_join_will_block = _state.orig_task_join_will_block
            result.task_join_will_block = _state.orig_task_join_will_block

            @self.app.task(shared=False)
            def chain_add():
                return (self.add.s(4, 4) | self.add.s(8)).apply_async()

            r = chain_add.apply_async(throw=True).get()
            assert r.get() == 16
        finally:
            _state.task_join_will_block = fixture_task_join_will_block
            result.task_join_will_block = fixture_task_join_will_block

    def test_apply(self):
        x = chain(self.add.s(4, 4), self.add.s(8), self.add.s(10))
        res = x.apply()
        assert isinstance(res, EagerResult)
        assert res.get() == 26

        assert res.parent.get() == 16
        assert res.parent.parent.get() == 8
        assert res.parent.parent.parent is None

    def test_kwargs_apply(self):
        x = chain(self.add.s(), self.add.s(8), self.add.s(10))
        res = x.apply(kwargs={'x': 1, 'y': 1}).get()
        assert res == 20

    def test_single_expresion(self):
        x = chain(self.add.s(1, 2)).apply()
        assert x.get() == 3
        assert x.parent is None

    def test_empty_chain_returns_none(self):
        assert chain(app=self.app)() is None
        assert chain(app=self.app).apply_async() is None

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
        assert isinstance(chain.from_dict(x), _chain)
        x['args'] = (2,)
        assert isinstance(chain.from_dict(x), _chain)

    def test_accepts_generator_argument(self):
        x = chain(self.add.s(i) for i in range(10))
        assert x.tasks[0].type, self.add
        assert x.type

    def test_chord_sets_result_parent(self):
        g = (self.add.s(0, 0) |
             group(self.add.s(i, i) for i in range(1, 10)) |
             self.add.s(2, 2) |
             self.add.s(4, 4))
        res = g.freeze()

        assert isinstance(res, AsyncResult)
        assert not isinstance(res, GroupResult)
        assert isinstance(res.parent, AsyncResult)
        assert not isinstance(res.parent, GroupResult)
        assert isinstance(res.parent.parent, GroupResult)
        assert isinstance(res.parent.parent.parent, AsyncResult)
        assert not isinstance(res.parent.parent.parent, GroupResult)
        assert res.parent.parent.parent.parent is None

        seen = set()
        node = res
        while node:
            assert node.id not in seen
            seen.add(node.id)
            node = node.parent

    def test_append_to_empty_chain(self):
        x = chain()
        x |= self.add.s(1, 1)
        x |= self.add.s(1)
        x.freeze()
        tasks, _ = x._frozen
        assert len(tasks) == 2

        assert x.apply().get() == 3

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_chain_single_child_result(self):
        child_sig = self.add.si(1, 1)
        chain_sig = chain(child_sig)
        assert chain_sig.tasks[0] is child_sig

        with patch.object(
            # We want to get back the result of actually applying the task
            child_sig, "apply_async",
        ) as mock_apply, patch.object(
            # The child signature may be clone by `chain.prepare_steps()`
            child_sig, "clone", return_value=child_sig,
        ):
            res = chain_sig()
        # `_prepare_chain_from_options()` sets this `chain` kwarg with the
        # subsequent tasks which would be run - nothing in this case
        mock_apply.assert_called_once_with(chain=[])
        assert res is mock_apply.return_value

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_chain_single_child_group_result(self):
        child_sig = self.add.si(1, 1)
        # The group will `clone()` the child during instantiation so mock it
        with patch.object(child_sig, "clone", return_value=child_sig):
            group_sig = group(child_sig)
        # Now we can construct the chain signature which is actually under test
        chain_sig = chain(group_sig)
        assert chain_sig.tasks[0].tasks[0] is child_sig

        with patch.object(
            # We want to get back the result of actually applying the task
            child_sig, "apply_async",
        ) as mock_apply, patch.object(
            # The child signature may be clone by `chain.prepare_steps()`
            child_sig, "clone", return_value=child_sig,
        ):
            res = chain_sig()
        # `_prepare_chain_from_options()` sets this `chain` kwarg with the
        # subsequent tasks which would be run - nothing in this case
        mock_apply.assert_called_once_with(chain=[])
        assert res is mock_apply.return_value


class test_group(CanvasCase):

    def test_repr(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        assert repr(x)

    def test_reverse(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        assert isinstance(signature(x), group)
        assert isinstance(signature(dict(x)), group)

    def test_cannot_link_on_group(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        with pytest.raises(TypeError):
            x.apply_async(link=self.add.s(2, 2))

    def test_cannot_link_error_on_group(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        with pytest.raises(TypeError):
            x.apply_async(link_error=self.add.s(2, 2))

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

    def test_from_dict_deep_deserialize(self):
        original_group = group([self.add.s(1, 2)] * 42)
        serialized_group = json.loads(json.dumps(original_group))
        deserialized_group = group.from_dict(serialized_group)
        assert all(
            isinstance(child_task, Signature)
            for child_task in deserialized_group.tasks
        )

    def test_from_dict_deeper_deserialize(self):
        inner_group = group([self.add.s(1, 2)] * 42)
        outer_group = group([inner_group] * 42)
        serialized_group = json.loads(json.dumps(outer_group))
        deserialized_group = group.from_dict(serialized_group)
        assert all(
            isinstance(child_task, Signature)
            for child_task in deserialized_group.tasks
        )
        assert all(
            isinstance(grandchild_task, Signature)
            for child_task in deserialized_group.tasks
            for grandchild_task in child_task.tasks
        )

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
        assert list(iter(g)) == list(g.keys())

    def test_single_task(self):
        g = group([self.add.s(1, 1)])
        assert isinstance(g, group)
        assert len(g.tasks) == 1
        g = group(self.add.s(1, 1))
        assert isinstance(g, group)
        assert len(g.tasks) == 1

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
        res = x.apply(kwargs={'x': 1, 'y': 1}).get()
        assert res == [2, 2]

    def test_kwargs_apply_async(self):
        self.app.conf.task_always_eager = True
        x = group([self.add.s(), self.add.s()])
        res = self.helper_test_get_delay(
            x.apply_async(kwargs={'x': 1, 'y': 1})
        )
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

    def test_app_when_header_is_empty(self):
        x = chord([], self.add.s(4, 4))
        assert x.app is self.add.app

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_app_fallback_to_current(self):
        from celery._state import current_app
        t1 = Mock(name='t1')
        t1.app = t1._app = None
        x = chord([t1], body=t1)
        assert x.app is current_app

    def test_chord_size_simple(self):
        sig = chord(self.add.s())
        assert sig.__length_hint__() == 1

    def test_chord_size_with_body(self):
        sig = chord(self.add.s(), self.add.s())
        assert sig.__length_hint__() == 1

    def test_chord_size_explicit_group_single(self):
        sig = chord(group(self.add.s()))
        assert sig.__length_hint__() == 1

    def test_chord_size_explicit_group_many(self):
        sig = chord(group([self.add.s()] * 42))
        assert sig.__length_hint__() == 42

    def test_chord_size_implicit_group_single(self):
        sig = chord([self.add.s()])
        assert sig.__length_hint__() == 1

    def test_chord_size_implicit_group_many(self):
        sig = chord([self.add.s()] * 42)
        assert sig.__length_hint__() == 42

    def test_chord_size_chain_single(self):
        sig = chord(chain(self.add.s()))
        assert sig.__length_hint__() == 1

    def test_chord_size_chain_many(self):
        # Chains get flattened into the encapsulating chord so even though the
        # chain would only count for 1, the tasks we pulled into the chord's
        # header and are counted as a bunch of simple signature objects
        sig = chord(chain([self.add.s()] * 42))
        assert sig.__length_hint__() == 42

    def test_chord_size_nested_chain_chain_single(self):
        sig = chord(chain(chain(self.add.s())))
        assert sig.__length_hint__() == 1

    def test_chord_size_nested_chain_chain_many(self):
        # The outer chain will be pulled up into the chord but the lower one
        # remains and will only count as a single final element
        sig = chord(chain(chain([self.add.s()] * 42)))
        assert sig.__length_hint__() == 1

    def test_chord_size_implicit_chain_single(self):
        sig = chord([self.add.s()])
        assert sig.__length_hint__() == 1

    def test_chord_size_implicit_chain_many(self):
        # This isn't a chain object so the `tasks` attribute can't be lifted
        # into the chord - this isn't actually valid and would blow up we tried
        # to run it but it sanity checks our recursion
        sig = chord([[self.add.s()] * 42])
        assert sig.__length_hint__() == 1

    def test_chord_size_nested_implicit_chain_chain_single(self):
        sig = chord([chain(self.add.s())])
        assert sig.__length_hint__() == 1

    def test_chord_size_nested_implicit_chain_chain_many(self):
        sig = chord([chain([self.add.s()] * 42)])
        assert sig.__length_hint__() == 1

    def test_chord_size_nested_chord_body_simple(self):
        sig = chord(chord(tuple(), self.add.s()))
        assert sig.__length_hint__() == 1

    def test_chord_size_nested_chord_body_implicit_group_single(self):
        sig = chord(chord(tuple(), [self.add.s()]))
        assert sig.__length_hint__() == 1

    def test_chord_size_nested_chord_body_implicit_group_many(self):
        sig = chord(chord(tuple(), [self.add.s()] * 42))
        assert sig.__length_hint__() == 42

    # Nested groups in a chain only affect the chord size if they are the last
    # element in the chain - in that case each group element is counted
    def test_chord_size_nested_group_chain_group_head_single(self):
        x = chord(
            group(
                [group(self.add.s()) | self.add.s()] * 42
            ),
            body=self.add.s()
        )
        assert x.__length_hint__() == 42

    def test_chord_size_nested_group_chain_group_head_many(self):
        x = chord(
            group(
                [group([self.add.s()] * 4) | self.add.s()] * 2
            ),
            body=self.add.s()
        )
        assert x.__length_hint__() == 2

    def test_chord_size_nested_group_chain_group_mid_single(self):
        x = chord(
            group(
                [self.add.s() | group(self.add.s()) | self.add.s()] * 42
            ),
            body=self.add.s()
        )
        assert x.__length_hint__() == 42

    def test_chord_size_nested_group_chain_group_mid_many(self):
        x = chord(
            group(
                [self.add.s() | group([self.add.s()] * 4) | self.add.s()] * 2
            ),
            body=self.add.s()
        )
        assert x.__length_hint__() == 2

    def test_chord_size_nested_group_chain_group_tail_single(self):
        x = chord(
            group(
                [self.add.s() | group(self.add.s())] * 42
            ),
            body=self.add.s()
        )
        assert x.__length_hint__() == 42

    def test_chord_size_nested_group_chain_group_tail_many(self):
        x = chord(
            group(
                [self.add.s() | group([self.add.s()] * 4)] * 2
            ),
            body=self.add.s()
        )
        assert x.__length_hint__() == 4 * 2

    def test_chord_size_nested_implicit_group_chain_group_tail_single(self):
        x = chord(
            [self.add.s() | group(self.add.s())] * 42,
            body=self.add.s()
        )
        assert x.__length_hint__() == 42

    def test_chord_size_nested_implicit_group_chain_group_tail_many(self):
        x = chord(
            [self.add.s() | group([self.add.s()] * 4)] * 2,
            body=self.add.s()
        )
        assert x.__length_hint__() == 4 * 2

    def test_chord_size_deserialized_element_single(self):
        child_sig = self.add.s()
        deserialized_child_sig = json.loads(json.dumps(child_sig))
        # We have to break in to be sure that a child remains as a `dict` so we
        # can confirm that the length hint will instantiate a `Signature`
        # object and then descend as expected
        chord_sig = chord(tuple())
        chord_sig.tasks = [deserialized_child_sig]
        with patch(
            "celery.canvas.Signature.from_dict", return_value=child_sig
        ) as mock_from_dict:
            assert chord_sig. __length_hint__() == 1
        mock_from_dict.assert_called_once_with(deserialized_child_sig)

    def test_chord_size_deserialized_element_many(self):
        child_sig = self.add.s()
        deserialized_child_sig = json.loads(json.dumps(child_sig))
        # We have to break in to be sure that a child remains as a `dict` so we
        # can confirm that the length hint will instantiate a `Signature`
        # object and then descend as expected
        chord_sig = chord(tuple())
        chord_sig.tasks = [deserialized_child_sig] * 42
        with patch(
            "celery.canvas.Signature.from_dict", return_value=child_sig
        ) as mock_from_dict:
            assert chord_sig. __length_hint__() == 42
        mock_from_dict.assert_has_calls([call(deserialized_child_sig)] * 42)

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

    def test_freeze_tasks_body_is_group(self, subtests):
        # Confirm that `group index` values counting up from 0 are set for
        # elements of a chord's body when the chord is encapsulated in a group
        body_elem = self.add.s()
        chord_body = group([body_elem] * 42)
        chord_obj = chord(self.add.s(), body=chord_body)
        top_group = group([chord_obj])
        # We expect the body to be the signature we passed in before we freeze
        with subtests.test(msg="Validate body tasks are retained"):
            assert all(
                embedded_body_elem is body_elem
                for embedded_body_elem in chord_obj.body.tasks
            )
        # We also expect the body to have no initial options - since all of the
        # embedded body elements are confirmed to be `body_elem` this is valid
        assert body_elem.options == {}
        # When we freeze the chord, its body will be cloned and options set
        top_group.freeze()
        with subtests.test(
            msg="Validate body group indicies count from 0 after freezing"
        ):
            assert all(
                embedded_body_elem is not body_elem
                for embedded_body_elem in chord_obj.body.tasks
            )
            assert all(
                embedded_body_elem.options["group_index"] == i
                for i, embedded_body_elem in enumerate(chord_obj.body.tasks)
            )

    def test_freeze_tasks_is_not_group(self):
        x = chord([self.add.s(2, 2)], body=self.add.s(), app=self.app)
        x.freeze()
        x.tasks = [self.add.s(2, 2)]
        x.freeze()

    def test_chain_always_eager(self):
        self.app.conf.task_always_eager = True
        from celery import _state, result

        fixture_task_join_will_block = _state.task_join_will_block
        try:
            _state.task_join_will_block = _state.orig_task_join_will_block
            result.task_join_will_block = _state.orig_task_join_will_block

            @self.app.task(shared=False)
            def finalize(*args):
                pass

            @self.app.task(shared=False)
            def chord_add():
                return chord([self.add.s(4, 4)], finalize.s()).apply_async()

            chord_add.apply_async(throw=True).get()
        finally:
            _state.task_join_will_block = fixture_task_join_will_block
            result.task_join_will_block = fixture_task_join_will_block

    def test_from_dict(self):
        header = self.add.s(1, 2)
        original_chord = chord(header=header)
        rebuilt_chord = chord.from_dict(dict(original_chord))
        assert isinstance(rebuilt_chord, chord)

    def test_from_dict_with_body(self):
        header = body = self.add.s(1, 2)
        original_chord = chord(header=header, body=body)
        rebuilt_chord = chord.from_dict(dict(original_chord))
        assert isinstance(rebuilt_chord, chord)

    def test_from_dict_deep_deserialize(self, subtests):
        header = body = self.add.s(1, 2)
        original_chord = chord(header=header, body=body)
        serialized_chord = json.loads(json.dumps(original_chord))
        deserialized_chord = chord.from_dict(serialized_chord)
        with subtests.test(msg="Verify chord is deserialized"):
            assert isinstance(deserialized_chord, chord)
        with subtests.test(msg="Validate chord header tasks is deserialized"):
            assert all(
                isinstance(child_task, Signature)
                for child_task in deserialized_chord.tasks
            )
        with subtests.test(msg="Verify chord body is deserialized"):
            assert isinstance(deserialized_chord.body, Signature)

    def test_from_dict_deep_deserialize_group(self, subtests):
        header = body = group([self.add.s(1, 2)] * 42)
        original_chord = chord(header=header, body=body)
        serialized_chord = json.loads(json.dumps(original_chord))
        deserialized_chord = chord.from_dict(serialized_chord)
        with subtests.test(msg="Verify chord is deserialized"):
            assert isinstance(deserialized_chord, chord)
        # A header which is a group gets unpacked into the chord's `tasks`
        with subtests.test(
            msg="Validate chord header tasks are deserialized and unpacked"
        ):
            assert all(
                isinstance(child_task, Signature)
                and not isinstance(child_task, group)
                for child_task in deserialized_chord.tasks
            )
        # A body which is a group remains as it we passed in
        with subtests.test(
            msg="Validate chord body is deserialized and not unpacked"
        ):
            assert isinstance(deserialized_chord.body, group)
            assert all(
                isinstance(body_child_task, Signature)
                for body_child_task in deserialized_chord.body.tasks
            )

    def test_from_dict_deeper_deserialize_group(self, subtests):
        inner_group = group([self.add.s(1, 2)] * 42)
        header = body = group([inner_group] * 42)
        original_chord = chord(header=header, body=body)
        serialized_chord = json.loads(json.dumps(original_chord))
        deserialized_chord = chord.from_dict(serialized_chord)
        with subtests.test(msg="Verify chord is deserialized"):
            assert isinstance(deserialized_chord, chord)
        # A header which is a group gets unpacked into the chord's `tasks`
        with subtests.test(
            msg="Validate chord header tasks are deserialized and unpacked"
        ):
            assert all(
                isinstance(child_task, group)
                for child_task in deserialized_chord.tasks
            )
            assert all(
                isinstance(grandchild_task, Signature)
                for child_task in deserialized_chord.tasks
                for grandchild_task in child_task.tasks
            )
        # A body which is a group remains as it we passed in
        with subtests.test(
            msg="Validate chord body is deserialized and not unpacked"
        ):
            assert isinstance(deserialized_chord.body, group)
            assert all(
                isinstance(body_child_task, group)
                for body_child_task in deserialized_chord.body.tasks
            )
            assert all(
                isinstance(body_grandchild_task, Signature)
                for body_child_task in deserialized_chord.body.tasks
                for body_grandchild_task in body_child_task.tasks
            )

    def test_from_dict_deep_deserialize_chain(self, subtests):
        header = body = chain([self.add.s(1, 2)] * 42)
        original_chord = chord(header=header, body=body)
        serialized_chord = json.loads(json.dumps(original_chord))
        deserialized_chord = chord.from_dict(serialized_chord)
        with subtests.test(msg="Verify chord is deserialized"):
            assert isinstance(deserialized_chord, chord)
        # A header which is a chain gets unpacked into the chord's `tasks`
        with subtests.test(
            msg="Validate chord header tasks are deserialized and unpacked"
        ):
            assert all(
                isinstance(child_task, Signature)
                and not isinstance(child_task, chain)
                for child_task in deserialized_chord.tasks
            )
        # A body which is a chain gets mutatated into the hidden `_chain` class
        with subtests.test(
            msg="Validate chord body is deserialized and not unpacked"
        ):
            assert isinstance(deserialized_chord.body, _chain)


class test_maybe_signature(CanvasCase):

    def test_is_None(self):
        assert maybe_signature(None, app=self.app) is None

    def test_is_dict(self):
        assert isinstance(maybe_signature(dict(self.add.s()), app=self.app),
                          Signature)

    def test_when_sig(self):
        s = self.add.s()
        assert maybe_signature(s, app=self.app) is s
