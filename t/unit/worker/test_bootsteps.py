from __future__ import absolute_import, unicode_literals

import pytest
from case import Mock, patch

from celery import bootsteps


class test_StepFormatter:

    def test_get_prefix(self):
        f = bootsteps.StepFormatter()
        s = Mock()
        s.last = True
        assert f._get_prefix(s) == f.blueprint_prefix

        s2 = Mock()
        s2.last = False
        s2.conditional = True
        assert f._get_prefix(s2) == f.conditional_prefix

        s3 = Mock()
        s3.last = s3.conditional = False
        assert f._get_prefix(s3) == ''

    def test_node(self):
        f = bootsteps.StepFormatter()
        f.draw_node = Mock()
        step = Mock()
        step.last = False
        f.node(step, x=3)
        f.draw_node.assert_called_with(step, f.node_scheme, {'x': 3})

        step.last = True
        f.node(step, x=3)
        f.draw_node.assert_called_with(step, f.blueprint_scheme, {'x': 3})

    def test_edge(self):
        f = bootsteps.StepFormatter()
        f.draw_edge = Mock()
        a, b = Mock(), Mock()
        a.last = True
        f.edge(a, b, x=6)
        f.draw_edge.assert_called_with(a, b, f.edge_scheme, {
            'x': 6, 'arrowhead': 'none', 'color': 'darkseagreen3',
        })

        a.last = False
        f.edge(a, b, x=6)
        f.draw_edge.assert_called_with(a, b, f.edge_scheme, {
            'x': 6,
        })


class test_Step:

    class Def(bootsteps.StartStopStep):
        name = 'test_Step.Def'

    def setup(self):
        self.steps = []

    def test_blueprint_name(self, bp='test_blueprint_name'):

        class X(bootsteps.Step):
            blueprint = bp
            name = 'X'
        assert X.name == 'X'

        class Y(bootsteps.Step):
            name = '%s.Y' % bp
        assert Y.name == '{0}.Y'.format(bp)

    def test_init(self):
        assert self.Def(self)

    def test_create(self):
        self.Def(self).create(self)

    def test_include_if(self):
        x = self.Def(self)
        x.enabled = True
        assert x.include_if(self)

        x.enabled = False
        assert not x.include_if(self)

    def test_instantiate(self):
        assert isinstance(
            self.Def(self).instantiate(self.Def, self),
            self.Def,
        )

    def test_include_when_enabled(self):
        x = self.Def(self)
        x.create = Mock()
        x.create.return_value = 'George'
        assert x.include(self)

        assert x.obj == 'George'
        x.create.assert_called_with(self)

    def test_include_when_disabled(self):
        x = self.Def(self)
        x.enabled = False
        x.create = Mock()

        assert not x.include(self)
        x.create.assert_not_called()

    def test_repr(self):
        x = self.Def(self)
        assert repr(x)


class test_ConsumerStep:

    def test_interface(self):
        step = bootsteps.ConsumerStep(self)
        with pytest.raises(NotImplementedError):
            step.get_consumers(self)

    def test_start_stop_shutdown(self):
        consumer = Mock()
        self.connection = Mock()

        class Step(bootsteps.ConsumerStep):

            def get_consumers(self, c):
                return [consumer]

        step = Step(self)
        assert step.get_consumers(self) == [consumer]

        step.start(self)
        consumer.consume.assert_called_with()
        step.stop(self)
        consumer.cancel.assert_called_with()

        step.shutdown(self)
        consumer.channel.close.assert_called_with()

    def test_start_no_consumers(self):
        self.connection = Mock()

        class Step(bootsteps.ConsumerStep):

            def get_consumers(self, c):
                return ()

        step = Step(self)
        step.start(self)

    def test_close_no_consumer_channel(self):
        step = bootsteps.ConsumerStep(Mock())
        step.consumers = [Mock()]
        step.consumers[0].channel = None
        step._close(Mock())


class test_StartStopStep:

    class Def(bootsteps.StartStopStep):
        name = 'test_StartStopStep.Def'

    def setup(self):
        self.steps = []

    def test_start__stop(self):
        x = self.Def(self)
        x.create = Mock()

        # include creates the underlying object and sets
        # its x.obj attribute to it, as well as appending
        # it to the parent.steps list.
        x.include(self)
        assert self.steps
        assert self.steps[0] is x

        x.start(self)
        x.obj.start.assert_called_with()

        x.stop(self)
        x.obj.stop.assert_called_with()

        x.obj = None
        assert x.start(self) is None

    def test_terminate__no_obj(self):
        x = self.Def(self)
        x.obj = None
        x.terminate(Mock())

    def test_include_when_disabled(self):
        x = self.Def(self)
        x.enabled = False
        x.include(self)
        assert not self.steps

    def test_terminate(self):
        x = self.Def(self)
        x.create = Mock()

        x.include(self)
        delattr(x.obj, 'terminate')
        x.terminate(self)
        x.obj.stop.assert_called_with()


class test_Blueprint:

    class Blueprint(bootsteps.Blueprint):
        name = 'test_Blueprint'

    def test_steps_added_to_unclaimed(self):

        class tnA(bootsteps.Step):
            name = 'test_Blueprint.A'

        class tnB(bootsteps.Step):
            name = 'test_Blueprint.B'

        class xxA(bootsteps.Step):
            name = 'xx.A'

        class Blueprint(self.Blueprint):
            default_steps = [tnA, tnB]
        blueprint = Blueprint()

        assert tnA in blueprint.types
        assert tnB in blueprint.types
        assert xxA not in blueprint.types

    def test_init(self):
        blueprint = self.Blueprint()
        assert blueprint.name == 'test_Blueprint'

    def test_close__on_close_is_None(self):
        blueprint = self.Blueprint()
        blueprint.on_close = None
        blueprint.send_all = Mock()
        blueprint.close(1)
        blueprint.send_all.assert_called_with(
            1, 'close', 'closing', reverse=False,
        )

    def test_send_all_with_None_steps(self):
        parent = Mock()
        blueprint = self.Blueprint()
        parent.steps = [None, None, None]
        blueprint.send_all(parent, 'close', 'Closing', reverse=False)

    def test_send_all_raises(self):
        parent = Mock()
        blueprint = self.Blueprint()
        parent.steps = [Mock()]
        parent.steps[0].foo.side_effect = KeyError()
        blueprint.send_all(parent, 'foo', propagate=False)
        with pytest.raises(KeyError):
            blueprint.send_all(parent, 'foo', propagate=True)

    def test_stop_state_in_TERMINATE(self):
        blueprint = self.Blueprint()
        blueprint.state = bootsteps.TERMINATE
        blueprint.stop(Mock())

    def test_join_raises_IGNORE_ERRORS(self):
        prev, bootsteps.IGNORE_ERRORS = bootsteps.IGNORE_ERRORS, (KeyError,)
        try:
            blueprint = self.Blueprint()
            blueprint.shutdown_complete = Mock()
            blueprint.shutdown_complete.wait.side_effect = KeyError('luke')
            blueprint.join(timeout=10)
            blueprint.shutdown_complete.wait.assert_called_with(timeout=10)
        finally:
            bootsteps.IGNORE_ERRORS = prev

    def test_connect_with(self):

        class b1s1(bootsteps.Step):
            pass

        class b1s2(bootsteps.Step):
            last = True

        class b2s1(bootsteps.Step):
            pass

        class b2s2(bootsteps.Step):
            last = True

        b1 = self.Blueprint([b1s1, b1s2])
        b2 = self.Blueprint([b2s1, b2s2])
        b1.apply(Mock())
        b2.apply(Mock())
        b1.connect_with(b2)

        assert b1s1 in b1.graph
        assert b2s1 in b1.graph
        assert b2s2 in b1.graph

        assert repr(b1s1)
        assert str(b1s1)

    def test_topsort_raises_KeyError(self):

        class Step(bootsteps.Step):
            requires = ('xyxxx.fsdasewe.Unknown',)

        b = self.Blueprint([Step])
        b.steps = b.claim_steps()
        with pytest.raises(ImportError):
            b._finalize_steps(b.steps)
        Step.requires = ()

        b.steps = b.claim_steps()
        b._finalize_steps(b.steps)

        with patch('celery.bootsteps.DependencyGraph') as Dep:
            g = Dep.return_value = Mock()
            g.topsort.side_effect = KeyError('foo')
            with pytest.raises(KeyError):
                b._finalize_steps(b.steps)

    def test_apply(self):

        class MyBlueprint(bootsteps.Blueprint):
            name = 'test_apply'

            def modules(self):
                return ['A', 'B']

        class B(bootsteps.Step):
            name = 'test_apply.B'

        class C(bootsteps.Step):
            name = 'test_apply.C'
            requires = [B]

        class A(bootsteps.Step):
            name = 'test_apply.A'
            requires = [C]

        class D(bootsteps.Step):
            name = 'test_apply.D'
            last = True

        x = MyBlueprint([A, D])
        x.apply(self)

        assert isinstance(x.order[0], B)
        assert isinstance(x.order[1], C)
        assert isinstance(x.order[2], A)
        assert isinstance(x.order[3], D)
        assert A in x.types
        assert x[A.name] is x.order[2]

    def test_find_last_but_no_steps(self):

        class MyBlueprint(bootsteps.Blueprint):
            name = 'qwejwioqjewoqiej'

        x = MyBlueprint()
        x.apply(self)
        assert x._find_last() is None
