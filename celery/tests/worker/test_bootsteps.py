from __future__ import absolute_import

from mock import Mock

from celery import bootsteps

from celery.tests.utils import AppCase, Case


class test_Step(Case):

    class Def(bootsteps.Step):
        name = 'test_Step.Def'

    def test_steps_must_be_named(self):
        with self.assertRaises(NotImplementedError):

            class X(bootsteps.Step):
                pass

        class Y(bootsteps.Step):
            abstract = True

    def test_namespace_name(self, ns='test_namespace_name'):

        class X(bootsteps.Step):
            namespace = ns
            name = 'X'
        self.assertEqual(X.namespace, ns)
        self.assertEqual(X.name, 'X')

        class Y(bootsteps.Step):
            name = '%s.Y' % (ns, )
        self.assertEqual(Y.namespace, ns)
        self.assertEqual(Y.name, 'Y')

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


class test_Namespace(AppCase):

    class NS(bootsteps.Namespace):
        name = 'test_Namespace'

    class ImportingNS(bootsteps.Namespace):

        def __init__(self, *args, **kwargs):
            bootsteps.Namespace.__init__(self, *args, **kwargs)
            self.imported = []

        def modules(self):
            return ['A', 'B', 'C']

        def import_module(self, module):
            self.imported.append(module)

    def test_steps_added_to_unclaimed(self):

        class tnA(bootsteps.Step):
            name = 'test_Namespace.A'

        class tnB(bootsteps.Step):
            name = 'test_Namespace.B'

        class xxA(bootsteps.Step):
            name = 'xx.A'

        self.assertIn('A', self.NS._unclaimed['test_Namespace'])
        self.assertIn('B', self.NS._unclaimed['test_Namespace'])
        self.assertIn('A', self.NS._unclaimed['xx'])
        self.assertNotIn('B', self.NS._unclaimed['xx'])

    def test_init(self):
        ns = self.NS(app=self.app)
        self.assertIs(ns.app, self.app)
        self.assertEqual(ns.name, 'test_Namespace')
        self.assertFalse(ns.services)

    def test_interface_modules(self):
        self.NS(app=self.app).modules()

    def test_load_modules(self):
        x = self.ImportingNS(app=self.app)
        x.load_modules()
        self.assertListEqual(x.imported, ['A', 'B', 'C'])

    def test_apply(self):

        class MyNS(bootsteps.Namespace):
            name = 'test_apply'

            def modules(self):
                return ['A', 'B']

        class A(bootsteps.Step):
            name = 'test_apply.A'
            requires = ['C']

        class B(bootsteps.Step):
            name = 'test_apply.B'

        class C(bootsteps.Step):
            name = 'test_apply.C'
            requires = ['B']

        class D(bootsteps.Step):
            name = 'test_apply.D'
            last = True

        x = MyNS(app=self.app)
        x.import_module = Mock()
        x.apply(self)

        self.assertItemsEqual(x.steps.values(), [A, B, C, D])
        self.assertTrue(x.import_module.call_count)

        for boot_step in x.boot_steps:
            self.assertEqual(boot_step.namespace, x)

        self.assertIsInstance(x.boot_steps[0], B)
        self.assertIsInstance(x.boot_steps[1], C)
        self.assertIsInstance(x.boot_steps[2], A)
        self.assertIsInstance(x.boot_steps[3], D)

        self.assertIs(x['A'], A)

    def test_import_module(self):
        x = self.NS(app=self.app)
        import os
        self.assertIs(x.import_module('os'), os)

    def test_find_last_but_no_steps(self):

        class MyNS(bootsteps.Namespace):
            name = 'qwejwioqjewoqiej'

        x = MyNS(app=self.app)
        x.apply(self)
        self.assertIsNone(x._find_last())
