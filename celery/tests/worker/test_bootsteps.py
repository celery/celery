from __future__ import absolute_import
from __future__ import with_statement

from mock import Mock

from celery.worker import bootsteps

from celery.tests.utils import AppCase, Case


class test_Component(Case):

    class Def(bootsteps.Component):
        name = 'test_Component.Def'

    def test_components_must_be_named(self):
        with self.assertRaises(NotImplementedError):

            class X(bootsteps.Component):
                pass

        class Y(bootsteps.Component):
            abstract = True

    def test_namespace_name(self, ns='test_namespace_name'):

        class X(bootsteps.Component):
            namespace = ns
            name = 'X'
        self.assertEqual(X.namespace, ns)
        self.assertEqual(X.name, 'X')

        class Y(bootsteps.Component):
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


class test_StartStopComponent(Case):

    class Def(bootsteps.StartStopComponent):
        name = 'test_StartStopComponent.Def'

    def setUp(self):
        self.components = []

    def test_start__stop(self):
        x = self.Def(self)
        x.create = Mock()

        # include creates the underlying object and sets
        # its x.obj attribute to it, as well as appending
        # it to the parent.components list.
        x.include(self)
        self.assertTrue(self.components)
        self.assertIs(self.components[0], x.obj)

        x.start()
        x.obj.start.assert_called_with()

        x.stop()
        x.obj.stop.assert_called_with()

    def test_include_when_disabled(self):
        x = self.Def(self)
        x.enabled = False
        x.include(self)
        self.assertFalse(self.components)

    def test_terminate_when_terminable(self):
        x = self.Def(self)
        x.terminable = True
        x.create = Mock()

        x.include(self)
        x.terminate()
        x.obj.terminate.assert_called_with()
        self.assertFalse(x.obj.stop.call_count)

    def test_terminate_calls_stop_when_not_terminable(self):
        x = self.Def(self)
        x.terminable = False
        x.create = Mock()

        x.include(self)
        x.terminate()
        x.obj.stop.assert_called_with()
        self.assertFalse(x.obj.terminate.call_count)


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

    def test_components_added_to_unclaimed(self):

        class tnA(bootsteps.Component):
            name = 'test_Namespace.A'

        class tnB(bootsteps.Component):
            name = 'test_Namespace.B'

        class xxA(bootsteps.Component):
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

        class A(bootsteps.Component):
            name = 'test_apply.A'
            requires = ['C']

        class B(bootsteps.Component):
            name = 'test_apply.B'

        class C(bootsteps.Component):
            name = 'test_apply.C'
            requires = ['B']

        class D(bootsteps.Component):
            name = 'test_apply.D'
            last = True

        x = MyNS(app=self.app)
        x.import_module = Mock()
        x.apply(self)

        self.assertItemsEqual(x.components.values(), [A, B, C, D])
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

    def test_find_last_but_no_components(self):

        class MyNS(bootsteps.Namespace):
            name = 'qwejwioqjewoqiej'

        x = MyNS(app=self.app)
        x.apply(self)
        self.assertIsNone(x._find_last())
