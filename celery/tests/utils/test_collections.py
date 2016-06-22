from __future__ import absolute_import, unicode_literals

import pickle

from collections import Mapping
from itertools import count

from billiard.einfo import ExceptionInfo
from time import time

from celery.utils.collections import (
    AttributeDict,
    BufferMap,
    ConfigurationView,
    DictAttribute,
    LimitedSet,
    Messagebuffer,
)
from celery.five import items
from celery.utils.objects import Bunch

from celery.tests.case import Case, skip


class test_DictAttribute(Case):

    def test_get_set_keys_values_items(self):
        x = DictAttribute(Bunch())
        x['foo'] = 'The quick brown fox'
        self.assertEqual(x['foo'], 'The quick brown fox')
        self.assertEqual(x['foo'], x.obj.foo)
        self.assertEqual(x.get('foo'), 'The quick brown fox')
        self.assertIsNone(x.get('bar'))
        with self.assertRaises(KeyError):
            x['bar']
        x.foo = 'The quick yellow fox'
        self.assertEqual(x['foo'], 'The quick yellow fox')
        self.assertIn(
            ('foo', 'The quick yellow fox'),
            list(x.items()),
        )
        self.assertIn('foo', list(x.keys()))
        self.assertIn('The quick yellow fox', list(x.values()))

    def test_setdefault(self):
        x = DictAttribute(Bunch())
        x.setdefault('foo', 'NEW')
        self.assertEqual(x['foo'], 'NEW')
        x.setdefault('foo', 'XYZ')
        self.assertEqual(x['foo'], 'NEW')

    def test_contains(self):
        x = DictAttribute(Bunch())
        x['foo'] = 1
        self.assertIn('foo', x)
        self.assertNotIn('bar', x)

    def test_items(self):
        obj = Bunch(attr1=1)
        x = DictAttribute(obj)
        x['attr2'] = 2
        self.assertEqual(x['attr1'], 1)
        self.assertEqual(x['attr2'], 2)


class test_ConfigurationView(Case):

    def setUp(self):
        self.view = ConfigurationView({'changed_key': 1,
                                       'both': 2},
                                      [{'default_key': 1,
                                       'both': 1}])

    def test_setdefault(self):
        self.view.setdefault('both', 36)
        self.assertEqual(self.view['both'], 2)
        self.view.setdefault('new', 36)
        self.assertEqual(self.view['new'], 36)

    def test_get(self):
        self.assertEqual(self.view.get('both'), 2)
        sp = object()
        self.assertIs(self.view.get('nonexisting', sp), sp)

    def test_update(self):
        changes = dict(self.view.changes)
        self.view.update(a=1, b=2, c=3)
        self.assertDictEqual(self.view.changes,
                             dict(changes, a=1, b=2, c=3))

    def test_contains(self):
        self.assertIn('changed_key', self.view)
        self.assertIn('default_key', self.view)
        self.assertNotIn('new', self.view)

    def test_repr(self):
        self.assertIn('changed_key', repr(self.view))
        self.assertIn('default_key', repr(self.view))

    def test_iter(self):
        expected = {'changed_key': 1,
                    'default_key': 1,
                    'both': 2}
        self.assertDictEqual(dict(items(self.view)), expected)
        self.assertItemsEqual(list(iter(self.view)),
                              list(expected.keys()))
        self.assertItemsEqual(list(self.view.keys()), list(expected.keys()))
        self.assertItemsEqual(
            list(self.view.values()),
            list(expected.values()),
        )
        self.assertIn('changed_key', list(self.view.keys()))
        self.assertIn(2, list(self.view.values()))
        self.assertIn(('both', 2), list(self.view.items()))

    def test_add_defaults_dict(self):
        defaults = {'foo': 10}
        self.view.add_defaults(defaults)
        self.assertEqual(self.view.foo, 10)

    def test_add_defaults_object(self):
        defaults = Bunch(foo=10)
        self.view.add_defaults(defaults)
        self.assertEqual(self.view.foo, 10)

    def test_clear(self):
        self.view.clear()
        self.assertEqual(self.view.both, 1)
        self.assertNotIn('changed_key', self.view)

    def test_bool(self):
        self.assertTrue(bool(self.view))
        self.view.maps[:] = []
        self.assertFalse(bool(self.view))

    def test_len(self):
        self.assertEqual(len(self.view), 3)
        self.view.KEY = 33
        self.assertEqual(len(self.view), 4)
        self.view.clear()
        self.assertEqual(len(self.view), 2)

    def test_isa_mapping(self):
        from collections import Mapping
        self.assertTrue(issubclass(ConfigurationView, Mapping))

    def test_isa_mutable_mapping(self):
        from collections import MutableMapping
        self.assertTrue(issubclass(ConfigurationView, MutableMapping))


class test_ExceptionInfo(Case):

    def test_exception_info(self):

        try:
            raise LookupError('The quick brown fox jumps...')
        except Exception:
            einfo = ExceptionInfo()
            self.assertEqual(str(einfo), einfo.traceback)
            self.assertIsInstance(einfo.exception, LookupError)
            self.assertTupleEqual(
                einfo.exception.args, ('The quick brown fox jumps...',),
            )
            self.assertTrue(einfo.traceback)

            r = repr(einfo)
            self.assertTrue(r)


@skip.if_win32()
class test_LimitedSet(Case):

    def test_add(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.add('bar')
        for n in 'foo', 'bar':
            self.assertIn(n, s)
        s.add('baz')
        for n in 'bar', 'baz':
            self.assertIn(n, s)
        self.assertNotIn('foo', s)

        s = LimitedSet(maxlen=10)
        for i in range(150):
            s.add(i)
        self.assertLessEqual(len(s), 10)

        # make sure heap is not leaking:
        self.assertLessEqual(
            len(s._heap),
            len(s) * (100. + s.max_heap_percent_overload) / 100,
        )

    def test_purge(self):
        # purge now enforces rules
        # cant purge(1) now. but .purge(now=...) still works
        s = LimitedSet(maxlen=10)
        [s.add(i) for i in range(10)]
        s.maxlen = 2
        s.purge()
        self.assertEqual(len(s), 2)

        # expired
        s = LimitedSet(maxlen=10, expires=1)
        [s.add(i) for i in range(10)]
        s.maxlen = 2
        s.purge(now=time() + 100)
        self.assertEqual(len(s), 0)

        # not expired
        s = LimitedSet(maxlen=None, expires=1)
        [s.add(i) for i in range(10)]
        s.maxlen = 2
        s.purge(now=lambda: time() - 100)
        self.assertEqual(len(s), 2)

        # expired -> minsize
        s = LimitedSet(maxlen=10, minlen=10, expires=1)
        [s.add(i) for i in range(20)]
        s.minlen = 3
        s.purge(now=time() + 3)
        self.assertEqual(s.minlen, len(s))
        self.assertLessEqual(
            len(s._heap),
            s.maxlen * (100. + s.max_heap_percent_overload) / 100,
        )

    def test_pickleable(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.add('bar')
        self.assertEqual(pickle.loads(pickle.dumps(s)), s)

    def test_iter(self):
        s = LimitedSet(maxlen=3)
        items = ['foo', 'bar', 'baz', 'xaz']
        for item in items:
            s.add(item)
        l = list(iter(s))
        for item in items[1:]:
            self.assertIn(item, l)
        self.assertNotIn('foo', l)
        self.assertListEqual(l, items[1:], 'order by insertion time')

    def test_repr(self):
        s = LimitedSet(maxlen=2)
        items = 'foo', 'bar'
        for item in items:
            s.add(item)
        self.assertIn('LimitedSet(', repr(s))

    def test_discard(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.discard('foo')
        self.assertNotIn('foo', s)
        self.assertEqual(len(s._data), 0)
        s.discard('foo')

    def test_clear(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.add('bar')
        self.assertEqual(len(s), 2)
        s.clear()
        self.assertFalse(s)

    def test_update(self):
        s1 = LimitedSet(maxlen=2)
        s1.add('foo')
        s1.add('bar')

        s2 = LimitedSet(maxlen=2)
        s2.update(s1)
        self.assertItemsEqual(list(s2), ['foo', 'bar'])

        s2.update(['bla'])
        self.assertItemsEqual(list(s2), ['bla', 'bar'])

        s2.update(['do', 're'])
        self.assertItemsEqual(list(s2), ['do', 're'])
        s1 = LimitedSet(maxlen=10, expires=None)
        s2 = LimitedSet(maxlen=10, expires=None)
        s3 = LimitedSet(maxlen=10, expires=None)
        s4 = LimitedSet(maxlen=10, expires=None)
        s5 = LimitedSet(maxlen=10, expires=None)
        for i in range(12):
            s1.add(i)
            s2.add(i * i)
        s3.update(s1)
        s3.update(s2)
        s4.update(s1.as_dict())
        s4.update(s2.as_dict())
        s5.update(s1._data)  # revoke is using this
        s5.update(s2._data)
        self.assertEqual(s3, s4)
        self.assertEqual(s3, s5)
        s2.update(s4)
        s4.update(s2)
        self.assertEqual(s2, s4)

    def test_iterable_and_ordering(self):
        s = LimitedSet(maxlen=35, expires=None)
        # we use a custom clock here, as time.time() does not have enough
        # precision when called quickly (can return the same value twice).
        clock = count(1)
        for i in reversed(range(15)):
            s.add(i, now=next(clock))
        j = 40
        for i in s:
            self.assertLess(i, j)  # each item is smaller and smaller
            j = i
        self.assertEqual(i, 0)  # last item is zero

    def test_pop_and_ordering_again(self):
        s = LimitedSet(maxlen=5)
        for i in range(10):
            s.add(i)
        j = -1
        for _ in range(5):
            i = s.pop()
            self.assertLess(j, i)
        i = s.pop()
        self.assertEqual(i, None)

    def test_as_dict(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        self.assertIsInstance(s.as_dict(), Mapping)

    def test_add_removes_duplicate_from_small_heap(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.add('foo')
        s.add('foo')
        self.assertEqual(len(s), 1)
        self.assertEqual(len(s._data), 1)
        self.assertEqual(len(s._heap), 1)

    def test_add_removes_duplicate_from_big_heap(self):
        s = LimitedSet(maxlen=1000)
        [s.add(i) for i in range(2000)]
        self.assertEqual(len(s), 1000)
        [s.add('foo') for i in range(1000)]
        # heap is refreshed when 15% larger than _data
        self.assertLess(len(s._heap), 1150)
        [s.add('foo') for i in range(1000)]
        self.assertLess(len(s._heap), 1150)


class test_AttributeDict(Case):

    def test_getattr__setattr(self):
        x = AttributeDict({'foo': 'bar'})
        self.assertEqual(x['foo'], 'bar')
        with self.assertRaises(AttributeError):
            x.bar
        x.bar = 'foo'
        self.assertEqual(x['bar'], 'foo')


class test_Messagebuffer(Case):

    def assert_size_and_first(self, buf, size, expected_first_item):
        self.assertEqual(len(buf), size)
        self.assertEqual(buf.take(), expected_first_item)

    def test_append_limited(self):
        b = Messagebuffer(10)
        for i in range(20):
            b.put(i)
        self.assert_size_and_first(b, 10, 10)

    def test_append_unlimited(self):
        b = Messagebuffer(None)
        for i in range(20):
            b.put(i)
        self.assert_size_and_first(b, 20, 0)

    def test_extend_limited(self):
        b = Messagebuffer(10)
        b.extend(list(range(20)))
        self.assert_size_and_first(b, 10, 10)

    def test_extend_unlimited(self):
        b = Messagebuffer(None)
        b.extend(list(range(20)))
        self.assert_size_and_first(b, 20, 0)

    def test_extend_eviction_time_limited(self):
        b = Messagebuffer(3000)
        b.extend(range(10000))
        self.assertGreater(len(b), 3000)
        b.evict()
        self.assertEqual(len(b), 3000)

    def test_pop_empty_with_default(self):
        b = Messagebuffer(10)
        sentinel = object()
        self.assertIs(b.take(sentinel), sentinel)

    def test_pop_empty_no_default(self):
        b = Messagebuffer(10)
        with self.assertRaises(b.Empty):
            b.take()

    def test_repr(self):
        self.assertTrue(repr(Messagebuffer(10, [1, 2, 3])))

    def test_iter(self):
        b = Messagebuffer(10, list(range(10)))
        self.assertEqual(len(b), 10)
        for i, item in enumerate(b):
            self.assertEqual(item, i)
        self.assertEqual(len(b), 0)

    def test_contains(self):
        b = Messagebuffer(10, list(range(10)))
        self.assertIn(5, b)

    def test_reversed(self):
        self.assertEqual(
            list(reversed(Messagebuffer(10, list(range(10))))),
            list(reversed(range(10))),
        )

    def test_getitem(self):
        b = Messagebuffer(10, list(range(10)))
        for i in range(10):
            self.assertEqual(b[i], i)


class test_BufferMap(Case):

    def test_append_limited(self):
        b = BufferMap(10)
        for i in range(20):
            b.put(i, i)
        self.assert_size_and_first(b, 10, 10)

    def assert_size_and_first(self, buf, size, expected_first_item):
        self.assertEqual(buf.total, size)
        self.assertEqual(buf._LRUpop(), expected_first_item)

    def test_append_unlimited(self):
        b = BufferMap(None)
        for i in range(20):
            b.put(i, i)
        self.assert_size_and_first(b, 20, 0)

    def test_extend_limited(self):
        b = BufferMap(10)
        b.extend(1, list(range(20)))
        self.assert_size_and_first(b, 10, 10)

    def test_extend_unlimited(self):
        b = BufferMap(None)
        b.extend(1, list(range(20)))
        self.assert_size_and_first(b, 20, 0)

    def test_pop_empty_with_default(self):
        b = BufferMap(10)
        sentinel = object()
        self.assertIs(b.take(1, sentinel), sentinel)

    def test_pop_empty_no_default(self):
        b = BufferMap(10)
        with self.assertRaises(b.Empty):
            b.take(1)

    def test_repr(self):
        self.assertTrue(repr(Messagebuffer(10, [1, 2, 3])))
