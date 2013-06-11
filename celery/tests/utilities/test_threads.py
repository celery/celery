from __future__ import absolute_import

from mock import Mock, patch

from celery.utils.threads import (
    _LocalStack,
    _FastLocalStack,
    LocalManager,
    Local,
    bgThread,
)

from celery.tests.utils import Case, override_stdouts


class test_bgThread(Case):

    def test_crash(self):

        class T(bgThread):

            def body(self):
                raise KeyError()

        with patch('os._exit') as _exit:
            with override_stdouts():
                _exit.side_effect = ValueError()
                t = T()
                with self.assertRaises(ValueError):
                    t.run()
                _exit.assert_called_with(1)


class test_Local(Case):

    def test_iter(self):
        x = Local()
        x.foo = 'bar'
        ident = x.__ident_func__()
        self.assertIn((ident, {'foo': 'bar'}), list(iter(x)))

        delattr(x, 'foo')
        self.assertNotIn((ident, {'foo': 'bar'}), list(iter(x)))
        with self.assertRaises(AttributeError):
            delattr(x, 'foo')

        self.assertIsNotNone(x(lambda: 'foo'))


class test_LocalStack(Case):

    def test_stack(self):
        x = _LocalStack()
        self.assertIsNone(x.pop())
        x.__release_local__()
        ident = x.__ident_func__
        x.__ident_func__ = ident

        with self.assertRaises(RuntimeError):
            x()[0]

        x.push(['foo'])
        self.assertEqual(x()[0], 'foo')
        x.pop()
        with self.assertRaises(RuntimeError):
            x()[0]


class test_FastLocalStack(Case):

    def test_stack(self):
        x = _FastLocalStack()
        x.push(['foo'])
        x.push(['bar'])
        self.assertEqual(x.top, ['bar'])
        self.assertEqual(len(x), 2)
        x.pop()
        self.assertEqual(x.top, ['foo'])
        x.pop()
        self.assertIsNone(x.top)


