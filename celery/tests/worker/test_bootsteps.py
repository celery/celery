from __future__ import absolute_import

from mock import Mock

from celery import bootsteps

from celery.tests.utils import AppCase, Case


class test_Step(Case):

    class Def(bootsteps.StartStopStep):
        name = 'test_Step.Def'

    def setUp(self):
        self.steps = []

    def test_blueprint_name(self, bp='test_blueprint_name'):

        class X(bootsteps.Step):
            blueprint = bp
            name = 'X'
        self.assertEqual(X.name, 'X')

        class Y(bootsteps.Step):
            name = '%s.Y' % bp
        self.assertEqual(Y.name, '%s.Y' % bp)

    def test_init(self):
        self.assertTrue(self.Def(self))

    def test_create(self):
        self.Def(self).create(self)

    def test_include_if(self):
        x = self.Def(self)
        x.enabled = True
        self.assertTrue(x.include_if(self))

        x.enabled = False
        self.assertFalse(x.include_if(self))

    def test_instantiate(self):
        self.assertIsInstance(self.Def(self).instantiate(self.Def, self),
                              self.Def)

    def test_include_when_enabled(self):
        x = self.Def(self)
        x.create = Mock()
        x.create.return_value = 'George'
        self.assertTrue(x.include(self))

        self.assertEqual(x.obj, 'George')
        x.create.assert_called_with(self)

    def test_include_when_disabled(self):
        x = self.Def(self)
        x.enabled = False
        x.create = Mock()

        self.assertFalse(x.include(self))
        self.assertFalse(x.create.call_count)


class test_StartStopStep(Case):

    class Def(bootsteps.StartStopStep):
        name = 'test_StartStopStep.Def'

    def setUp(self):
        self.steps = []

    def test_start__stop(self):
        x = self.Def(self)
        x.create = Mock()

        # include creates the underlying object and sets
        # its x.obj attribute to it, as well as appending
        # it to the parent.steps list.
        x.include(self)
        self.assertTrue(self.steps)
        self.assertIs(self.steps[0], x)

        x.start(self)
        x.obj.start.assert_called_with()

        x.stop(self)
        x.obj.stop.assert_called_with()

    def test_include_when_disabled(self):
        x = self.Def(self)
        x.enabled = False
        x.include(self)
        self.assertFalse(self.steps)

    def test_terminate(self):
        x = self.Def(self)
        x.terminable = False
        x.create = Mock()

        x.include(self)
        x.terminate(self)
        x.obj.stop.assert_called_with()


class test_Blueprint(AppCase):

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
        blueprint = Blueprint(app=self.app)

        self.assertIn(tnA, blueprint._all_steps())
        self.assertIn(tnB, blueprint._all_steps())
        self.assertNotIn(xxA, blueprint._all_steps())

    def test_init(self):
        blueprint = self.Blueprint(app=self.app)
        self.assertIs(blueprint.app, self.app)
        self.assertEqual(blueprint.name, 'test_Blueprint')

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

        x = MyBlueprint([A, D], app=self.app)
        x.apply(self)

        self.assertIsInstance(x.order[0], B)
        self.assertIsInstance(x.order[1], C)
        self.assertIsInstance(x.order[2], A)
        self.assertIsInstance(x.order[3], D)
        self.assertIn(A, x.types)
        self.assertIs(x[A.name], x.order[2])

    def test_find_last_but_no_steps(self):

        class MyBlueprint(bootsteps.Blueprint):
            name = 'qwejwioqjewoqiej'

        x = MyBlueprint(app=self.app)
        x.apply(self)
        self.assertIsNone(x._find_last())
