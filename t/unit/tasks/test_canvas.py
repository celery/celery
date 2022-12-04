import json
import math
from unittest.mock import ANY, MagicMock, Mock, call, patch, sentinel

import pytest
import pytest_subtests  # noqa: F401

from celery import Task
from celery._state import _task_stack
from celery.canvas import (GroupStampingVisitor, Signature, StampingVisitor, _chain, _maybe_group, chain, chord,
                           chunks, group, maybe_signature, maybe_unroll_group, signature, xmap, xstarmap)
from celery.exceptions import Ignore
from celery.result import AsyncResult, EagerResult, GroupResult

SIG = Signature({
    'task': 'TASK',
    'args': ('A1',),
    'kwargs': {'K1': 'V1'},
    'options': {'task_id': 'TASK_ID'},
    'subtask_type': ''},
)
try:
    from collections.abc import Iterable
except ImportError:
    from collections.abc import Iterable


def return_True(*args, **kwargs):
    # Task run functions can't be closures/lambdas, as they're pickled.
    return True


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

    def setup_method(self):
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

        @self.app.task(shared=False)
        def xsum(numbers):
            return sum(sum(num) if isinstance(num, Iterable) else num for num in numbers)

        self.xsum = xsum

        @self.app.task(shared=False, bind=True)
        def replaced(self, x, y):
            return self.replace(add.si(x, y))

        self.replaced = replaced

        @self.app.task(shared=False, bind=True)
        def replaced_group(self, x, y):
            return self.replace(group(add.si(x, y), mul.si(x, y)))

        self.replaced_group = replaced_group

        @self.app.task(shared=False, bind=True)
        def replace_with_group(self, x, y):
            return self.replace(group(add.si(x, y), mul.si(x, y)))

        self.replace_with_group = replace_with_group

        @self.app.task(shared=False, bind=True)
        def replace_with_chain(self, x, y):
            return self.replace(group(add.si(x, y) | mul.s(y), add.si(x, y)))

        self.replace_with_chain = replace_with_chain

        @self.app.task(shared=False)
        def xprod(numbers):
            try:
                return math.prod(numbers)
            except AttributeError:
                #  TODO: Drop this backport once
                #        we drop support for Python 3.7
                import operator
                from functools import reduce

                return reduce(operator.mul, numbers)

        self.xprod = xprod


@Signature.register_type()
class chord_subclass(chord):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subtask_type = "chord_subclass"


@Signature.register_type()
class group_subclass(group):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subtask_type = "group_subclass"


@Signature.register_type()
class chain_subclass(chain):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subtask_type = "chain_subclass"


@Signature.register_type()
class chunks_subclass(chunks):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subtask_type = "chunks_subclass"


class test_Signature(CanvasCase):
    def test_double_stamping(self, subtests):
        """
        Test manual signature stamping with two different stamps.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_1.stamp(stamp1="stamp1")
        sig_1.stamp(stamp2="stamp2")
        sig_1_res = sig_1.freeze()
        sig_1.apply()

        with subtests.test("sig_1_res is stamped with stamp1", stamp1=["stamp1"]):
            assert sig_1_res._get_task_meta()["stamp1"] == ["stamp1"]

        with subtests.test("sig_1_res is stamped with stamp2", stamp2=["stamp2"]):
            assert sig_1_res._get_task_meta()["stamp2"] == ["stamp2"]

        with subtests.test("sig_1_res is stamped twice", stamped_headers=["stamp2", "stamp1"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["stamp2", "stamp1", "groups"])

    def test_twice_stamping(self, subtests):
        """
        Test manual signature stamping with two stamps twice.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_1.stamp(stamp="stamp1")
        sig_1.stamp(stamp="stamp2")
        sig_1_res = sig_1.freeze()
        sig_1.apply()

        with subtests.test("sig_1_res is stamped twice", stamps=["stamp2", "stamp1"]):
            assert sorted(sig_1_res._get_task_meta()["stamp"]) == sorted(["stamp2", "stamp1"])

        with subtests.test("sig_1_res is stamped twice", stamped_headers=["stamp2", "stamp1"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["stamp", "groups"])

    def test_manual_stamping(self):
        """
        Test manual signature stamping.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        stamps = ["stamp1", "stamp2"]
        sig_1.stamp(visitor=None, groups=[stamps[1]])
        sig_1.stamp(visitor=None, groups=stamps[0])
        sig_1_res = sig_1.freeze()
        sig_1.apply()
        assert sorted(sig_1_res._get_task_meta()['groups']) == sorted(stamps)

    def test_custom_stamping_visitor(self, subtests):
        """
        Test manual signature stamping with a custom visitor class.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        class CustomStampingVisitor1(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                # without using stamped_headers key explicitly
                # the key will be calculated from the headers implicitly
                return {'header': 'value'}

        class CustomStampingVisitor2(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value', 'stamped_headers': ['header']}

        sig_1 = self.add.s(2, 2)
        sig_1.stamp(visitor=CustomStampingVisitor1())
        sig_1_res = sig_1.freeze()
        sig_1.apply()
        sig_2 = self.add.s(2, 2)
        sig_2.stamp(visitor=CustomStampingVisitor2())
        sig_2_res = sig_2.freeze()
        sig_2.apply()

        with subtests.test("sig_1 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("sig_2 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("sig_1 is stamped with custom visitor", header=["value"]):
            assert sig_1_res._get_task_meta()["header"] == ["value"]

        with subtests.test("sig_2 is stamped with custom visitor", header=["value"]):
            assert sig_2_res._get_task_meta()["header"] == ["value"]

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_callback_stamping(self, subtests):
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value'}

            def on_callback(self, callback, **header) -> dict:
                return {'on_callback': True}

            def on_errback(self, errback, **header) -> dict:
                return {'on_errback': True}

        sig_1 = self.add.s(0, 1)
        sig_1_res = sig_1.freeze()
        group_sig = group([self.add.s(3), self.add.s(4)])
        group_sig_res = group_sig.freeze()
        chord_sig = chord([self.xsum.s(), self.xsum.s()], self.xsum.s())
        chord_sig_res = chord_sig.freeze()
        sig_2 = self.add.s(2)
        sig_2_res = sig_2.freeze()
        chain_sig = chain(
            sig_1,      # --> 1
            group_sig,  # --> [1+3, 1+4] --> [4, 5]
            chord_sig,  # --> [4+5, 4+5] --> [9, 9] --> 9+9 --> 18
            sig_2       # --> 18 + 2 --> 20
        )
        callback = signature('callback_task')
        errback = signature('errback_task')
        chain_sig.link(callback)
        chain_sig.link_error(errback)
        chain_sig.stamp(visitor=CustomStampingVisitor())
        chain_sig_res = chain_sig.apply_async()
        chain_sig_res.get()

        with subtests.test("Confirm the chain was executed correctly", result=20):
            # Before we run our assersions, let's confirm the base functionality of the chain is working
            # as expected including the links stamping.
            assert chain_sig_res.result == 20

        with subtests.test("sig_1 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("group_sig is stamped with custom visitor", stamped_headers=["header", "groups"]):
            for result in group_sig_res.results:
                assert sorted(result._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("chord_sig is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(chord_sig_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("sig_2 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("callback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_callback"]):
            callback_link = chain_sig.options['link'][0]
            headers = callback_link.options
            stamped_headers = headers['stamped_headers']
            assert sorted(stamped_headers) == sorted(["header", "groups", "on_callback"])
            assert headers['on_callback'] is True
            assert headers['header'] == 'value'

        with subtests.test("errback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_errback"]):
            errback_link = chain_sig.options['link_error'][0]
            headers = errback_link.options
            stamped_headers = headers['stamped_headers']
            assert sorted(stamped_headers) == sorted(["header", "groups", "on_errback"])
            assert headers['on_errback'] is True
            assert headers['header'] == 'value'

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_callback_stamping_on_replace(self, subtests):
        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value'}

            def on_callback(self, callback, **header) -> dict:
                return {'on_callback': True}

            def on_errback(self, errback, **header) -> dict:
                return {'on_errback': True}

        class MyTask(Task):
            def on_replace(self, sig):
                sig.stamp(CustomStampingVisitor())
                return super().on_replace(sig)

        mytask = self.app.task(shared=False, base=MyTask)(return_True)

        sig1 = signature('sig1')
        callback = signature('callback_task')
        errback = signature('errback_task')
        sig1.link(callback)
        sig1.link_error(errback)

        with subtests.test("callback is not stamped with custom visitor yet"):
            callback_link = sig1.options['link'][0]
            headers = callback_link.options
            assert 'on_callback' not in headers
            assert 'header' not in headers

        with subtests.test("errback is not stamped with custom visitor yet"):
            errback_link = sig1.options['link_error'][0]
            headers = errback_link.options
            assert 'on_errback' not in headers
            assert 'header' not in headers

        with pytest.raises(Ignore):
            mytask.replace(sig1)

        with subtests.test("callback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_callback"]):
            callback_link = sig1.options['link'][0]
            headers = callback_link.options
            stamped_headers = headers['stamped_headers']
            assert sorted(stamped_headers) == sorted(["header", "groups", "on_callback"])
            assert headers['on_callback'] is True
            assert headers['header'] == 'value'

        with subtests.test("errback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_errback"]):
            errback_link = sig1.options['link_error'][0]
            headers = errback_link.options
            stamped_headers = headers['stamped_headers']
            assert sorted(stamped_headers) == sorted(["header", "groups", "on_errback"])
            assert headers['on_errback'] is True
            assert headers['header'] == 'value'

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
        x = Signature('TASK', ('A',), {})
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

    def test_chunks_preserves_state(self):
        x = self.add.chunks(range(100), 10)
        d = dict(x)
        d['subtask_type'] = "chunks_subclass"
        isinstance(chunks_subclass.from_dict(d), chunks_subclass)
        isinstance(chunks_subclass.from_dict(d).clone(), chunks_subclass)

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

    @pytest.mark.parametrize("chain_type", (_chain, chain_subclass))
    def test_clone_preserves_state(self, chain_type):
        x = chain_type(self.add.s(i, i) for i in range(10))
        assert x.clone().tasks == x.tasks
        assert x.clone().kwargs == x.kwargs
        assert x.clone().args == x.args
        assert isinstance(x.clone(), chain_type)

    def test_repr(self):
        x = self.add.s(2, 2) | self.add.s(2)
        assert repr(x) == f'{self.add.name}(2, 2) | add(2)'

    def test_apply_async(self):
        c = self.add.s(2, 2) | self.add.s(4) | self.add.s(8)
        result = c.apply_async()
        assert result.parent
        assert result.parent.parent
        assert result.parent.parent.parent is None

    @pytest.mark.parametrize("chain_type", (_chain, chain_subclass))
    def test_splices_chains(self, chain_type):
        c = chain_type(
            self.add.s(5, 5),
            chain_type(self.add.s(6), self.add.s(7), self.add.s(8), app=self.app),
            app=self.app,
        )
        c.freeze()
        tasks, _ = c._frozen
        assert len(tasks) == 4
        assert isinstance(c, chain_type)

    @pytest.mark.parametrize("chain_type", [_chain, chain_subclass])
    def test_from_dict_no_tasks(self, chain_type):
        assert chain_type.from_dict(dict(chain_type(app=self.app)), app=self.app)
        assert isinstance(chain_type.from_dict(dict(chain_type(app=self.app)), app=self.app), chain_type)

    @pytest.mark.parametrize("chain_type", [_chain, chain_subclass])
    def test_from_dict_full_subtasks(self, chain_type):
        c = chain_type(self.add.si(1, 2), self.add.si(3, 4), self.add.si(5, 6))
        serialized = json.loads(json.dumps(c))
        deserialized = chain_type.from_dict(serialized)
        assert all(isinstance(task, Signature) for task in deserialized.tasks)
        assert isinstance(deserialized, chain_type)

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

    def test_chord_to_chain(self):
        c = (
            chord([self.add.s('x0', 'y0'), self.add.s('x1', 'y1')],
                  self.add.s(['foo'])) |
            chain(self.add.s(['y']), self.add.s(['z']))
        )
        assert isinstance(c, _chain)
        assert c.apply().get() == ['x0y0', 'x1y1', 'foo', 'y', 'z']

    def test_chord_to_group(self):
        c = (
            chord([self.add.s('x0', 'y0'), self.add.s('x1', 'y1')],
                  self.add.s(['foo'])) |
            group([self.add.s(['y']), self.add.s(['z'])])
        )
        assert isinstance(c, _chain)
        assert c.apply().get() == [
            ['x0y0', 'x1y1', 'foo', 'y'],
            ['x0y0', 'x1y1', 'foo', 'z']
        ]

    def test_chain_of_chord__or__group_of_single_task(self):
        c = chord([signature('header')], signature('body'))
        c = chain(c)
        g = group(signature('t'))
        new_chain = c | g  # g should be chained with the body of c[0]
        assert isinstance(new_chain, _chain)
        assert isinstance(new_chain.tasks[0].body, _chain)

    def test_chain_of_chord_upgrade_on_chaining(self):
        c = chord([signature('header')], group(signature('body')))
        c = chain(c)
        t = signature('t')
        new_chain = c | t  # t should be chained with the body of c[0] and create a new chord
        assert isinstance(new_chain, _chain)
        assert isinstance(new_chain.tasks[0].body, chord)

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
        expected_sig = s(32)
        expected_sig.stamp(visitor=GroupStampingVisitor())
        assert c.tasks[-1].options['link'] == [expected_sig]

        c.apply_async(link_error=[s('error')])
        expected_sig = s('error')
        expected_sig.stamp(visitor=GroupStampingVisitor())
        for task in c.tasks:
            assert task.options['link_error'] == [expected_sig]

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

    def test_chain_flattening_keep_links_of_inner_chain(self):
        def link_chain(sig):
            sig.link(signature('link_b'))
            sig.link_error(signature('link_ab'))
            return sig

        inner_chain = link_chain(chain(signature('a'), signature('b')))
        assert inner_chain.options['link'][0] == signature('link_b')
        assert inner_chain.options['link_error'][0] == signature('link_ab')
        assert inner_chain.tasks[0] == signature('a')
        assert inner_chain.tasks[0].options == {}
        assert inner_chain.tasks[1] == signature('b')
        assert inner_chain.tasks[1].options == {}

        flat_chain = chain(inner_chain, signature('c'))
        assert flat_chain.options == {}
        assert flat_chain.tasks[0].name == 'a'
        assert 'link' not in flat_chain.tasks[0].options
        assert signature(flat_chain.tasks[0].options['link_error'][0]) == signature('link_ab')
        assert flat_chain.tasks[1].name == 'b'
        assert 'link' in flat_chain.tasks[1].options, "b is missing the link from inner_chain.options['link'][0]"
        assert signature(flat_chain.tasks[1].options['link'][0]) == signature('link_b')
        assert signature(flat_chain.tasks[1].options['link_error'][0]) == signature('link_ab')


class test_group(CanvasCase):
    def test_group_stamping_one_level(self, subtests):
        """
        Test that when a group ID is frozen, that group ID is stored in
        each task within the group.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(4, 4)
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g.stamp(stamp="stamp")
        g_res = g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped", groups=[g_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g_res.id]

        with subtests.test("sig_1_res is stamped manually", stamp=["stamp"]):
            assert sig_1_res._get_task_meta()['stamp'] == ["stamp"]

        with subtests.test("sig_2_res is stamped", groups=[g_res.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g_res.id]

        with subtests.test("sig_2_res is stamped manually", stamp=["stamp"]):
            assert sig_2_res._get_task_meta()['stamp'] == ["stamp"]

        with subtests.test("sig_1_res has stamped_headers", stamped_headers=["stamp", 'groups']):
            assert sorted(sig_1_res._get_task_meta()['stamped_headers']) == sorted(['stamp', 'groups'])

        with subtests.test("sig_2_res has stamped_headers", stamped_headers=["stamp"]):
            assert sorted(sig_2_res._get_task_meta()['stamped_headers']) == sorted(['stamp', 'groups'])

    def test_group_stamping_two_levels(self, subtests):
        """
        For a group within a group, test that group stamps are stored in
        the correct order.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(1, 1)
        nested_sig_1 = self.add.s(2)
        nested_sig_2 = self.add.s(4)

        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()
        first_nested_sig_res = nested_sig_1.freeze()
        second_nested_sig_res = nested_sig_2.freeze()

        g2 = group(
            nested_sig_1,
            nested_sig_2,
            app=self.app
        )

        g2_res = g2.freeze()

        g1 = group(
            sig_1,
            chain(
                sig_2,
                g2,
                app=self.app
            ),
            app=self.app
        )

        g1_res = g1.freeze()
        g1.apply()

        with subtests.test("sig_1_res is stamped", groups=[g1_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g1_res.id]
        with subtests.test("sig_2_res is stamped", groups=[g1_res.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g1_res.id]
        with subtests.test("first_nested_sig_res is stamped", groups=[g1_res.id, g2_res.id]):
            assert sorted(first_nested_sig_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])
        with subtests.test("second_nested_sig_res is stamped", groups=[g1_res.id, g2_res.id]):
            assert sorted(second_nested_sig_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])

    def test_group_stamping_with_replace(self, subtests):
        """
        For a group within a replaced element, test that group stamps are replaced correctly.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(2, 2) | self.replaced.s(8)
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g_res = g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped", groups=[g_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g_res.id]
        with subtests.test("sig_2_res is stamped", groups=[g_res.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g_res.id]

    def test_group_stamping_with_replaced_group(self, subtests):
        """
        For a group within a replaced element, test that group stamps are replaced correctly.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True
        nested_g = self.replace_with_group.s(8)
        nested_g_res = nested_g.freeze()
        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(2, 2) | nested_g
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g_res = g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped", groups=[g_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g_res.id]
        with subtests.test("sig_2_res is stamped", groups=nested_g_res._get_task_meta()['groups']):
            assert sig_2_res._get_task_meta()['groups'] == nested_g_res._get_task_meta()['groups']

    def test_group_stamping_with_replaced_chain(self, subtests):
        """
        For a group within a replaced element, test that group stamps are replaced correctly.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True
        nested_g = self.replace_with_chain.s(8)
        nested_g_res = nested_g.freeze()
        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(2, 2) | nested_g
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g_res = g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped", groups=[g_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g_res.id]
        with subtests.test("sig_2_res is stamped", groups=nested_g_res._get_task_meta()['groups']):
            assert sig_2_res._get_task_meta()['groups'] == nested_g_res._get_task_meta()['groups']

    def test_group_stamping_three_levels(self, subtests):
        """
        For groups with three levels of nesting, test that group stamps
        are saved in the correct order for all nesting levels.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_in_g1_1 = self.add.s(2, 2)
        sig_in_g1_2 = self.add.s(1, 1)
        sig_in_g2 = self.add.s(2)
        sig_in_g2_chain = self.add.s(4)
        sig_in_g3_1 = self.add.s(8)
        sig_in_g3_2 = self.add.s(16)

        sig_in_g1_1_res = sig_in_g1_1.freeze()
        sig_in_g1_2_res = sig_in_g1_2.freeze()
        sig_in_g2_res = sig_in_g2.freeze()
        sig_in_g2_chain_res = sig_in_g2_chain.freeze()
        sig_in_g3_1_res = sig_in_g3_1.freeze()
        sig_in_g3_2_res = sig_in_g3_2.freeze()

        g3 = group(
            sig_in_g3_1,
            sig_in_g3_2,
            app=self.app
        )

        g3_res = g3.freeze()

        g2 = group(
            sig_in_g2,
            chain(
                sig_in_g2_chain,
                g3
            ),
            app=self.app
        )

        g2_res = g2.freeze()

        g1 = group(
            sig_in_g1_1,
            chain(
                sig_in_g1_2,
                g2,
                app=self.app
            ),
            app=self.app
        )

        g1_res = g1.freeze()
        g1.apply()

        with subtests.test("sig_in_g1_1_res is stamped", groups=[g1_res.id]):
            assert sig_in_g1_1_res._get_task_meta()['groups'] == [g1_res.id]
        with subtests.test("sig_in_g1_2_res is stamped", groups=[g1_res.id]):
            assert sig_in_g1_2_res._get_task_meta()['groups'] == [g1_res.id]
        with subtests.test("sig_in_g2_res is stamped", groups=[g1_res.id, g2_res.id]):
            assert sorted(sig_in_g2_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])
        with subtests.test("sig_in_g2_chain_res is stamped", groups=[g1_res.id, g2_res.id]):
            assert sorted(sig_in_g2_chain_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])
        with subtests.test("sig_in_g3_1_res is stamped", groups=[g1_res.id, g2_res.id, g3_res.id]):
            assert sorted(sig_in_g3_1_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id, g3_res.id])
        with subtests.test("sig_in_g3_2_res is stamped", groups=[g1_res.id, g2_res.id, g3_res.id]):
            assert sorted(sig_in_g3_2_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id, g3_res.id])

    def test_group_stamping_parallel_groups(self, subtests):
        """
        In the case of group within a group that is from another canvas
        element, ensure that group stamps are added correctly when groups are
        run in parallel.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_in_g1 = self.add.s(1, 1)
        sig_in_g2_chain = self.add.s(2, 2)
        sig_in_g2_1 = self.add.s(4)
        sig_in_g2_2 = self.add.s(8)
        sig_in_g3_chain = self.add.s(2, 2)
        sig_in_g3_1 = self.add.s(4)
        sig_in_g3_2 = self.add.s(8)

        sig_in_g1_res = sig_in_g1.freeze(_id='sig_in_g1')
        sig_in_g2_chain_res = sig_in_g2_chain.freeze(_id='sig_in_g2_chain')
        sig_in_g2_1_res = sig_in_g2_1.freeze(_id='sig_in_g2_1')
        sig_in_g2_2_res = sig_in_g2_2.freeze(_id='sig_in_g2_2')
        sig_in_g3_chain_res = sig_in_g3_chain.freeze(_id='sig_in_g3_chain')
        sig_in_g3_1_res = sig_in_g3_1.freeze(_id='sig_in_g3_1')
        sig_in_g3_2_res = sig_in_g3_2.freeze(_id='sig_in_g3_2')

        g3 = group(
            sig_in_g3_1,
            sig_in_g3_2,
            app=self.app
        )
        g3_res = g3.freeze(group_id='g3')

        g2 = group(
            sig_in_g2_1,
            sig_in_g2_2,
            app=self.app
        )
        g2_res = g2.freeze(group_id='g2')

        g1 = group(
            sig_in_g1,
            chain(
                sig_in_g2_chain,
                g2,
                app=self.app
            ),
            chain(
                sig_in_g3_chain,
                g3,
                app=self.app
            ),
        )
        g1_res = g1.freeze(group_id='g1')
        g1.apply()

        with subtests.test("sig_in_g1 is stamped", groups=[g1_res.id]):
            assert sig_in_g1_res.id == 'sig_in_g1'
            assert sig_in_g1_res._get_task_meta()['groups'] == [g1_res.id]

        with subtests.test("sig_in_g2_chain is stamped", groups=[g1_res.id]):
            assert sig_in_g2_chain_res.id == 'sig_in_g2_chain'
            assert sig_in_g2_chain_res._get_task_meta()['groups'] == \
                [g1_res.id]

        with subtests.test("sig_in_g2_1 is stamped", groups=[g1_res.id, g2_res.id]):
            assert sig_in_g2_1_res.id == 'sig_in_g2_1'
            assert sorted(sig_in_g2_1_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])

        with subtests.test("sig_in_g2_2 is stamped",
                           groups=[g1_res.id, g2_res.id]):
            assert sig_in_g2_2_res.id == 'sig_in_g2_2'
            assert sorted(sig_in_g2_2_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])

        with subtests.test("sig_in_g3_chain is stamped",
                           groups=[g1_res.id]):
            assert sig_in_g3_chain_res.id == 'sig_in_g3_chain'
            assert sig_in_g3_chain_res._get_task_meta()['groups'] == \
                [g1_res.id]

        with subtests.test("sig_in_g3_1 is stamped",
                           groups=[g1_res.id, g3_res.id]):
            assert sig_in_g3_1_res.id == 'sig_in_g3_1'
            assert sorted(sig_in_g3_1_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g3_res.id])

        with subtests.test("sig_in_g3_2 is stamped",
                           groups=[g1_res.id, g3_res.id]):
            assert sorted(sig_in_g3_2_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g3_res.id])

    def test_repr(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        assert repr(x)

    def test_reverse(self):
        x = group([self.add.s(2, 2), self.add.s(4, 4)])
        assert isinstance(signature(x), group)
        assert isinstance(signature(dict(x)), group)

    def test_reverse_with_subclass(self):
        x = group_subclass([self.add.s(2, 2), self.add.s(4, 4)])
        assert isinstance(signature(x), group_subclass)
        assert isinstance(signature(dict(x)), group_subclass)

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
        # Only the first child signature of a group will be given the callback
        # and it is cloned and made immutable to avoid passing results to it,
        # since that first task can't pass along its siblings' return values
        g1.tasks[0].link.assert_called_with(sig.clone().set(immutable=True))

    def test_link_error(self):
        g1 = group(Mock(name='t1'), Mock(name='t2'), app=self.app)
        sig = Mock(name='sig')
        g1.link_error(sig)
        # We expect that all group children will be given the errback to ensure
        # it gets called
        for child_sig in g1.tasks:
            child_sig.link_error.assert_called_with(sig)

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

    @pytest.mark.parametrize("group_type", (group, group_subclass))
    def test_from_dict(self, group_type):
        x = group_type([self.add.s(2, 2), self.add.s(4, 4)])
        x['args'] = (2, 2)
        value = group_type.from_dict(dict(x))
        assert value and isinstance(value, group_type)
        x['args'] = None
        value = group_type.from_dict(dict(x))
        assert value and isinstance(value, group_type)

    @pytest.mark.parametrize("group_type", (group, group_subclass))
    def test_from_dict_deep_deserialize(self, group_type):
        original_group = group_type([self.add.s(1, 2)] * 42)
        serialized_group = json.loads(json.dumps(original_group))
        deserialized_group = group_type.from_dict(serialized_group)
        assert isinstance(deserialized_group, group_type)
        assert all(
            isinstance(child_task, Signature)
            for child_task in deserialized_group.tasks
        )

    @pytest.mark.parametrize("group_type", (group, group_subclass))
    def test_from_dict_deeper_deserialize(self, group_type):
        inner_group = group_type([self.add.s(1, 2)] * 42)
        outer_group = group_type([inner_group] * 42)
        serialized_group = json.loads(json.dumps(outer_group))
        deserialized_group = group_type.from_dict(serialized_group)
        assert isinstance(deserialized_group, group_type)
        assert all(
            isinstance(child_task, group_type)
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

    def test_apply_from_generator(self):
        child_count = 42
        child_sig = self.add.si(0, 0)
        child_sigs_gen = (child_sig for _ in range(child_count))
        group_sig = group(child_sigs_gen)
        with patch("celery.canvas.Signature.apply_async") as mock_apply_async:
            res_obj = group_sig.apply_async()
        assert mock_apply_async.call_count == child_count
        assert len(res_obj.children) == child_count

    # This needs the current app for some reason not worth digging into
    @pytest.mark.usefixtures('depends_on_current_app')
    def test_apply_from_generator_empty(self):
        empty_gen = (False for _ in range(0))
        group_sig = group(empty_gen)
        with patch("celery.canvas.Signature.apply_async") as mock_apply_async:
            res_obj = group_sig.apply_async()
        assert mock_apply_async.call_count == 0
        assert len(res_obj.children) == 0

    # In the following tests, getting the group ID is a pain so we just use
    # `ANY` to wildcard it when we're checking on calls made to our mocks
    def test_apply_contains_chord(self):
        gchild_count = 42
        gchild_sig = self.add.si(0, 0)
        gchild_sigs = (gchild_sig,) * gchild_count
        child_chord = chord(gchild_sigs, gchild_sig)
        group_sig = group((child_chord,))
        with patch.object(
            self.app.backend, "set_chord_size",
        ) as mock_set_chord_size, patch(
            "celery.canvas.Signature.apply_async",
        ) as mock_apply_async:
            res_obj = group_sig.apply_async()
        # We only see applies for the header grandchildren because the tasks
        # are never actually run due to our mocking of `apply_async()`
        assert mock_apply_async.call_count == gchild_count
        assert len(res_obj.children) == len(group_sig.tasks)
        # We must have set the chord size for the group of tasks which makes up
        # the header of the `child_chord`, just before we apply the last task.
        mock_set_chord_size.assert_called_once_with(ANY, gchild_count)

    def test_apply_contains_chords_containing_chain(self):
        ggchild_count = 42
        ggchild_sig = self.add.si(0, 0)
        gchild_sig = chain((ggchild_sig,) * ggchild_count)
        child_count = 24
        child_chord = chord((gchild_sig,), ggchild_sig)
        group_sig = group((child_chord,) * child_count)
        with patch.object(
            self.app.backend, "set_chord_size",
        ) as mock_set_chord_size, patch(
            "celery.canvas.Signature.apply_async",
        ) as mock_apply_async:
            res_obj = group_sig.apply_async()
        # We only see applies for the header grandchildren because the tasks
        # are never actually run due to our mocking of `apply_async()`
        assert mock_apply_async.call_count == child_count
        assert len(res_obj.children) == child_count
        # We must have set the chord sizes based on the number of tail tasks of
        # the encapsulated chains - in this case 1 for each child chord
        mock_set_chord_size.assert_has_calls((call(ANY, 1),) * child_count)

    @pytest.mark.xfail(reason="Invalid canvas setup with bad exception")
    def test_apply_contains_chords_containing_empty_chain(self):
        gchild_sig = chain(tuple())
        child_count = 24
        child_chord = chord((gchild_sig,), self.add.si(0, 0))
        group_sig = group((child_chord,) * child_count)
        # This is an invalid setup because we can't complete a chord header if
        # there are no actual tasks which will run in it. However, the current
        # behaviour of an `IndexError` isn't particularly helpful to a user.
        group_sig.apply_async()

    def test_apply_contains_chords_containing_chain_with_empty_tail(self):
        ggchild_count = 42
        ggchild_sig = self.add.si(0, 0)
        tail_count = 24
        gchild_sig = chain(
            (ggchild_sig,) * ggchild_count +
            (group((ggchild_sig,) * tail_count), group(tuple()),),
        )
        child_chord = chord((gchild_sig,), ggchild_sig)
        group_sig = group((child_chord,))
        with patch.object(
            self.app.backend, "set_chord_size",
        ) as mock_set_chord_size, patch(
            "celery.canvas.Signature.apply_async",
        ) as mock_apply_async:
            res_obj = group_sig.apply_async()
        # We only see applies for the header grandchildren because the tasks
        # are never actually run due to our mocking of `apply_async()`
        assert mock_apply_async.call_count == 1
        assert len(res_obj.children) == 1
        # We must have set the chord sizes based on the size of the last
        # non-empty task in the encapsulated chains - in this case `tail_count`
        # for the group preceding the empty one in each grandchild chain
        mock_set_chord_size.assert_called_once_with(ANY, tail_count)

    def test_apply_contains_chords_containing_group(self):
        ggchild_count = 42
        ggchild_sig = self.add.si(0, 0)
        gchild_sig = group((ggchild_sig,) * ggchild_count)
        child_count = 24
        child_chord = chord((gchild_sig,), ggchild_sig)
        group_sig = group((child_chord,) * child_count)
        with patch.object(
            self.app.backend, "set_chord_size",
        ) as mock_set_chord_size, patch(
            "celery.canvas.Signature.apply_async",
        ) as mock_apply_async:
            res_obj = group_sig.apply_async()
        # We see applies for all of the header grandchildren because the tasks
        # are never actually run due to our mocking of `apply_async()`
        assert mock_apply_async.call_count == child_count * ggchild_count
        assert len(res_obj.children) == child_count
        # We must have set the chord sizes based on the number of tail tasks of
        # the encapsulated groups - in this case `ggchild_count`
        mock_set_chord_size.assert_has_calls(
            (call(ANY, ggchild_count),) * child_count,
        )

    @pytest.mark.xfail(reason="Invalid canvas setup but poor behaviour")
    def test_apply_contains_chords_containing_empty_group(self):
        gchild_sig = group(tuple())
        child_count = 24
        child_chord = chord((gchild_sig,), self.add.si(0, 0))
        group_sig = group((child_chord,) * child_count)
        with patch.object(
            self.app.backend, "set_chord_size",
        ) as mock_set_chord_size, patch(
            "celery.canvas.Signature.apply_async",
        ) as mock_apply_async:
            res_obj = group_sig.apply_async()
        # We only see applies for the header grandchildren because the tasks
        # are never actually run due to our mocking of `apply_async()`
        assert mock_apply_async.call_count == child_count
        assert len(res_obj.children) == child_count
        # This is actually kind of meaningless because, similar to the empty
        # chain test, this is an invalid setup. However, we should probably
        # expect that the chords are dealt with in some other way the probably
        # being left incomplete forever...
        mock_set_chord_size.assert_has_calls((call(ANY, 0),) * child_count)

    def test_apply_contains_chords_containing_chord(self):
        ggchild_count = 42
        ggchild_sig = self.add.si(0, 0)
        gchild_sig = chord((ggchild_sig,) * ggchild_count, ggchild_sig)
        child_count = 24
        child_chord = chord((gchild_sig,), ggchild_sig)
        group_sig = group((child_chord,) * child_count)
        with patch.object(
            self.app.backend, "set_chord_size",
        ) as mock_set_chord_size, patch(
            "celery.canvas.Signature.apply_async",
        ) as mock_apply_async:
            res_obj = group_sig.apply_async()
        # We see applies for all of the header great-grandchildren because the
        # tasks are never actually run due to our mocking of `apply_async()`
        assert mock_apply_async.call_count == child_count * ggchild_count
        assert len(res_obj.children) == child_count
        # We must have set the chord sizes based on the number of tail tasks of
        # the deeply encapsulated chords' header tasks, as well as for each
        # child chord. This means we have `child_count` interleaved calls to
        # set chord sizes of 1 and `ggchild_count`.
        mock_set_chord_size.assert_has_calls(
            (call(ANY, 1), call(ANY, ggchild_count),) * child_count,
        )

    def test_apply_contains_chords_containing_empty_chord(self):
        gchild_sig = chord(tuple(), self.add.si(0, 0))
        child_count = 24
        child_chord = chord((gchild_sig,), self.add.si(0, 0))
        group_sig = group((child_chord,) * child_count)
        with patch.object(
            self.app.backend, "set_chord_size",
        ) as mock_set_chord_size, patch(
            "celery.canvas.Signature.apply_async",
        ) as mock_apply_async:
            res_obj = group_sig.apply_async()
        # We only see applies for the header grandchildren because the tasks
        # are never actually run due to our mocking of `apply_async()`
        assert mock_apply_async.call_count == child_count
        assert len(res_obj.children) == child_count
        # We must have set the chord sizes based on the number of tail tasks of
        # the encapsulated chains - in this case 1 for each child chord
        mock_set_chord_size.assert_has_calls((call(ANY, 1),) * child_count)


class test_chord(CanvasCase):
    def test_chord_stamping_one_level(self, subtests):
        """
        In the case of group within a chord that is from another canvas
        element, ensure that chord stamps are added correctly when chord are
        run in parallel.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(4, 4)
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()
        sig_sum = self.xsum.s()
        sig_sum_res = sig_sum.freeze()

        g = chord([sig_1, sig_2], sig_sum, app=self.app)
        g.stamp(stamp="stamp")
        g.freeze()
        g.apply()

        with subtests.test("sig_sum_res body isn't stamped", groups=[]):
            assert sig_sum_res._get_task_meta()['groups'] == []

        with subtests.test("sig_1_res is stamped", groups=[g.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g.id]

        with subtests.test("sig_2_res is stamped", groups=[g.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g.id]

        with subtests.test("sig_1_res is stamped manually", stamp=["stamp"]):
            assert sig_1_res._get_task_meta()['stamp'] == ["stamp"]

        with subtests.test("sig_2_res is stamped manually", stamp=["stamp"]):
            assert sig_2_res._get_task_meta()['stamp'] == ["stamp"]

        with subtests.test("sig_1_res has stamped_headers", stamped_headers=["stamp", 'groups']):
            assert sorted(sig_1_res._get_task_meta()['stamped_headers']) == sorted(['stamp', 'groups'])

        with subtests.test("sig_2_res has stamped_headers", stamped_headers=["stamp", 'groups']):
            assert sorted(sig_2_res._get_task_meta()['stamped_headers']) == sorted(['stamp', 'groups'])

    def test_chord_stamping_two_levels(self, subtests):
        """
        For a group within a chord, test that group stamps are stored in
        the correct order.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(1, 1)
        nested_sig_1 = self.add.s(2)
        nested_sig_2 = self.add.s(4)

        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()
        first_nested_sig_res = nested_sig_1.freeze()
        second_nested_sig_res = nested_sig_2.freeze()

        g2 = group(
            nested_sig_1,
            nested_sig_2,
            app=self.app
        )

        g2_res = g2.freeze()

        sig_sum = self.xsum.s()
        sig_sum.freeze()

        g1 = chord([sig_2, chain(sig_1, g2)], sig_sum, app=self.app)

        g1.freeze()
        g1.apply()

        with subtests.test("sig_1_res body is stamped", groups=[g1.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g1.id]
        with subtests.test("sig_2_res body is stamped", groups=[g1.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g1.id]
        with subtests.test("first_nested_sig_res body is stamped", groups=[g1.id, g2_res.id]):
            assert sorted(first_nested_sig_res._get_task_meta()['groups']) == \
                sorted([g1.id, g2_res.id])
        with subtests.test("second_nested_sig_res body is stamped", groups=[g1.id, g2_res.id]):
            assert sorted(second_nested_sig_res._get_task_meta()['groups']) == \
                sorted([g1.id, g2_res.id])

    def test_chord_stamping_body_group(self, subtests):
        """
        In the case of group within a chord that is from another canvas
        element, ensure that chord stamps are added correctly when chord are
        run in parallel.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        tasks = [self.add.s(i, i) for i in range(10)]

        sum_task = self.xsum.s()
        sum_task_res = sum_task.freeze()
        prod_task = self.xprod.s()
        prod_task_res = sum_task.freeze()

        body = group(sum_task, prod_task)

        g = chord(tasks, body, app=self.app)
        g.freeze()
        g.apply()

        with subtests.test("sum_task_res is stamped", groups=[body.id]):
            assert sum_task_res._get_task_meta()['groups'] == [body.id]
        with subtests.test("prod_task_res is stamped", groups=[body.id]):
            assert prod_task_res._get_task_meta()['groups'] == [body.id]

    def test_chord_stamping_body_chord(self, subtests):
        """
        In the case of chord within a chord that is from another canvas
        element, ensure that chord stamps are added correctly when chord are
        run in parallel.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        parent_header_tasks = group([self.add.s(i, i) for i in range(10)])
        parent_header_tasks_res = parent_header_tasks.freeze()

        sum_task = self.xsum.s()
        sum_task_res = sum_task.freeze()
        sum_task2 = self.xsum.s()
        sum_task_res2 = sum_task2.freeze()
        prod_task = self.xprod.s()
        prod_task_res = sum_task.freeze()

        body = chord(group(sum_task, prod_task), sum_task2, app=self.app)

        c = chord(parent_header_tasks, body, app=self.app)
        c.freeze()
        c.apply()

        with subtests.test("parent_header_tasks are stamped", groups=[c.id]):
            for ar in parent_header_tasks_res.children:
                assert ar._get_task_meta()['groups'] == [c.id]
                assert ar._get_task_meta()['groups'] != [body.id]
        with subtests.test("sum_task_res is stamped", groups=[body.id]):
            assert sum_task_res._get_task_meta()['groups'] == [body.id]
            assert sum_task_res._get_task_meta()['groups'] != [c.id]
        with subtests.test("prod_task_res is stamped", groups=[body.id]):
            assert prod_task_res._get_task_meta()['groups'] == [body.id]
            assert prod_task_res._get_task_meta()['groups'] != [c.id]
        with subtests.test("sum_task_res2 is NOT stamped", groups=[]):
            assert len(sum_task_res2._get_task_meta()['groups']) == 0

    def test__get_app_does_not_exhaust_generator(self):
        def build_generator():
            yield self.add.s(1, 1)
            self.second_item_returned = True
            yield self.add.s(2, 2)
            raise pytest.fail("This should never be reached")

        self.second_item_returned = False
        c = chord(build_generator(), self.add.s(3))
        c.app
        # The second task gets returned due to lookahead in `regen()`
        assert self.second_item_returned
        # Access it again to make sure the generator is not further evaluated
        c.app

    @pytest.mark.parametrize("chord_type", [chord, chord_subclass])
    def test_reverse(self, chord_type):
        x = chord_type([self.add.s(2, 2), self.add.s(4, 4)], body=self.mul.s(4))
        assert isinstance(signature(x), chord_type)
        assert isinstance(signature(dict(x)), chord_type)

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
            assert chord_sig.__length_hint__() == 1
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
            assert chord_sig.__length_hint__() == 42
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

    @pytest.mark.parametrize("group_type", [group,  group_subclass])
    def test_freeze_tasks_body_is_group(self, subtests, group_type):
        # Confirm that `group index` values counting up from 0 are set for
        # elements of a chord's body when the chord is encapsulated in a group
        body_elem = self.add.s()
        chord_body = group_type([body_elem] * 42)
        chord_obj = chord(self.add.s(), body=chord_body)
        top_group = group_type([chord_obj])

        # We expect the body to be the signature we passed in before we freeze
        with subtests.test(msg="Validate body type and tasks are retained"):
            assert isinstance(chord_obj.body, group_type)
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
            msg="Validate body group indices count from 0 after freezing"
        ):
            assert isinstance(chord_obj.body, group_type)

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

    @pytest.mark.parametrize("chord_type", [chord, chord_subclass])
    def test_from_dict(self, chord_type):
        header = self.add.s(1, 2)
        original_chord = chord_type(header=header)
        rebuilt_chord = chord_type.from_dict(dict(original_chord))
        assert isinstance(rebuilt_chord, chord_type)

    @pytest.mark.parametrize("chord_type", [chord, chord_subclass])
    def test_from_dict_with_body(self, chord_type):
        header = body = self.add.s(1, 2)
        original_chord = chord_type(header=header, body=body)
        rebuilt_chord = chord_type.from_dict(dict(original_chord))
        assert isinstance(rebuilt_chord, chord_type)

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

    @pytest.mark.parametrize("group_type", [group, group_subclass])
    def test_from_dict_deep_deserialize_group(self, subtests, group_type):
        header = body = group_type([self.add.s(1, 2)] * 42)
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
                and not isinstance(child_task, group_type)
                for child_task in deserialized_chord.tasks
            )
        # A body which is a group remains as it we passed in
        with subtests.test(
            msg="Validate chord body is deserialized and not unpacked"
        ):
            assert isinstance(deserialized_chord.body, group_type)
            assert all(
                isinstance(body_child_task, Signature)
                for body_child_task in deserialized_chord.body.tasks
            )

    @pytest.mark.parametrize("group_type", [group, group_subclass])
    def test_from_dict_deeper_deserialize_group(self, subtests, group_type):
        inner_group = group_type([self.add.s(1, 2)] * 42)
        header = body = group_type([inner_group] * 42)
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
                isinstance(child_task, group_type)
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

    def test_chord_clone_kwargs(self, subtests):
        """ Test that chord clone ensures the kwargs are the same """

        with subtests.test(msg='Verify chord cloning clones kwargs correctly'):
            c = chord([signature('g'), signature('h')], signature('i'), kwargs={'U': 6})
            c2 = c.clone()
            assert c2.kwargs == c.kwargs

        with subtests.test(msg='Cloning the chord with overridden kwargs'):
            override_kw = {'X': 2}
            c3 = c.clone(args=(1,), kwargs=override_kw)

        with subtests.test(msg='Verify the overridden kwargs were cloned correctly'):
            new_kw = c.kwargs.copy()
            new_kw.update(override_kw)
            assert c3.kwargs == new_kw

    def test_flag_allow_error_cb_on_chord_header(self, subtests):
        header_mock = [Mock(name='t1'), Mock(name='t2')]
        header = group(header_mock)
        body = Mock(name='tbody')
        errback_sig = Mock(name='errback_sig')
        chord_sig = chord(header, body, app=self.app)

        with subtests.test(msg='Verify the errback is not linked'):
            # header
            for child_sig in header_mock:
                child_sig.link_error.assert_not_called()
            # body
            body.link_error.assert_not_called()

        with subtests.test(msg='Verify flag turned off links only the body'):
            self.app.conf.task_allow_error_cb_on_chord_header = False
            chord_sig.link_error(errback_sig)
            # header
            for child_sig in header_mock:
                child_sig.link_error.assert_not_called()
            # body
            body.link_error.assert_called_once_with(errback_sig)

        with subtests.test(msg='Verify flag turned on links the header'):
            self.app.conf.task_allow_error_cb_on_chord_header = True
            chord_sig.link_error(errback_sig)
            # header
            for child_sig in header_mock:
                child_sig.link_error.assert_called_once_with(errback_sig)
            # body
            body.link_error.assert_has_calls([call(errback_sig), call(errback_sig)])

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_flag_allow_error_cb_on_chord_header_various_header_types(self):
        """ Test chord link_error with various header types. """
        self.app.conf.task_allow_error_cb_on_chord_header = True
        headers = [
            signature('t'),
            [signature('t'), signature('t')],
            group(signature('t'), signature('t'))
        ]
        for chord_header in headers:
            c = chord(chord_header, signature('t'))
            sig = signature('t')
            errback = c.link_error(sig)
            assert errback == sig

    def test_chord__or__group_of_single_task(self):
        """ Test chaining a chord to a group of a single task. """
        c = chord([signature('header')], signature('body'))
        g = group(signature('t'))
        stil_chord = c | g  # g should be chained with the body of c
        assert isinstance(stil_chord, chord)
        assert isinstance(stil_chord.body, _chain)

    def test_chord_upgrade_on_chaining(self):
        """ Test that chaining a chord with a group body upgrades to a new chord """
        c = chord([signature('header')], group(signature('body')))
        t = signature('t')
        stil_chord = c | t  # t should be chained with the body of c and create a new chord
        assert isinstance(stil_chord, chord)
        assert isinstance(stil_chord.body, chord)


class test_maybe_signature(CanvasCase):

    def test_is_None(self):
        assert maybe_signature(None, app=self.app) is None

    def test_is_dict(self):
        assert isinstance(maybe_signature(dict(self.add.s()), app=self.app),
                          Signature)

    def test_when_sig(self):
        s = self.add.s()
        assert maybe_signature(s, app=self.app) is s
