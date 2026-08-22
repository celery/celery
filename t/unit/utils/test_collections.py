import pickle
from collections.abc import Mapping
from itertools import count
from time import monotonic
from unittest.mock import Mock

import pytest
from billiard.einfo import ExceptionInfo

import t.skip
from celery.utils.collections import (AttributeDict, BufferMap, ChainMap, ConfigurationView, DictAttribute,
                                      LimitedSet, Messagebuffer)
from celery.utils.objects import Bunch


class test_DictAttribute:

    def test_get_set_keys_values_items(self):
        x = DictAttribute(Bunch())
        x['foo'] = 'The quick brown fox'
        assert x['foo'] == 'The quick brown fox'
        assert x['foo'] == x.obj.foo
        assert x.get('foo') == 'The quick brown fox'
        assert x.get('bar') is None
        with pytest.raises(KeyError):
            x['bar']
        x.foo = 'The quick yellow fox'
        assert x['foo'] == 'The quick yellow fox'
        assert ('foo', 'The quick yellow fox') in list(x.items())
        assert 'foo' in list(x.keys())
        assert 'The quick yellow fox' in list(x.values())

    def test_setdefault(self):
        x = DictAttribute(Bunch())
        x.setdefault('foo', 'NEW')
        assert x['foo'] == 'NEW'
        x.setdefault('foo', 'XYZ')
        assert x['foo'] == 'NEW'

    def test_contains(self):
        x = DictAttribute(Bunch())
        x['foo'] = 1
        assert 'foo' in x
        assert 'bar' not in x

    def test_items(self):
        obj = Bunch(attr1=1)
        x = DictAttribute(obj)
        x['attr2'] = 2
        assert x['attr1'] == 1
        assert x['attr2'] == 2


class test_ConfigurationView:

    def setup_method(self):
        self.view = ConfigurationView(
            {'changed_key': 1, 'both': 2},
            [
                {'default_key': 1, 'both': 1},
            ],
        )

    def test_setdefault(self):
        self.view.setdefault('both', 36)
        assert self.view['both'] == 2
        self.view.setdefault('new', 36)
        assert self.view['new'] == 36

    def test_get(self):
        assert self.view.get('both') == 2
        sp = object()
        assert self.view.get('nonexisting', sp) is sp

    def test_update(self):
        changes = dict(self.view.changes)
        self.view.update(a=1, b=2, c=3)
        assert self.view.changes == dict(changes, a=1, b=2, c=3)

    def test_contains(self):
        assert 'changed_key' in self.view
        assert 'default_key' in self.view
        assert 'new' not in self.view

    def test_repr(self):
        assert 'changed_key' in repr(self.view)
        assert 'default_key' in repr(self.view)

    def test_iter(self):
        expected = {
            'changed_key': 1,
            'default_key': 1,
            'both': 2,
        }
        assert dict(self.view.items()) == expected
        assert sorted(list(iter(self.view))) == sorted(list(expected.keys()))
        assert sorted(list(self.view.keys())) == sorted(list(expected.keys()))
        assert (sorted(list(self.view.values())) ==
                sorted(list(expected.values())))
        assert 'changed_key' in list(self.view.keys())
        assert 2 in list(self.view.values())
        assert ('both', 2) in list(self.view.items())

    def test_add_defaults_dict(self):
        defaults = {'foo': 10}
        self.view.add_defaults(defaults)
        assert self.view.foo == 10

    def test_add_defaults_object(self):
        defaults = Bunch(foo=10)
        self.view.add_defaults(defaults)
        assert self.view.foo == 10

    def test_clear(self):
        self.view.clear()
        assert self.view.both == 1
        assert 'changed_key' not in self.view

    def test_bool(self):
        assert bool(self.view)
        self.view.maps[:] = []
        assert not bool(self.view)

    def test_len(self):
        assert len(self.view) == 3
        self.view.KEY = 33
        assert len(self.view) == 4
        self.view.clear()
        assert len(self.view) == 2

    def test_isa_mutable_mapping(self):
        from collections.abc import MutableMapping
        assert issubclass(ConfigurationView, MutableMapping)


class test_ExceptionInfo:

    def test_exception_info(self):

        try:
            raise LookupError('The quick brown fox jumps...')
        except Exception:
            einfo = ExceptionInfo()
            assert str(einfo) == einfo.traceback
            assert isinstance(einfo.exception.exc, LookupError)
            assert einfo.exception.exc.args == ('The quick brown fox jumps...',)
            assert einfo.traceback

            assert repr(einfo)


@t.skip.if_win32
class test_LimitedSet:

    def test_add(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.add('bar')
        for n in 'foo', 'bar':
            assert n in s
        s.add('baz')
        for n in 'bar', 'baz':
            assert n in s
        assert 'foo' not in s

        s = LimitedSet(maxlen=10)
        for i in range(150):
            s.add(i)
        assert len(s) <= 10

        # make sure heap is not leaking:
        assert len(s._heap) < len(s) * (
            100. + s.max_heap_percent_overload) / 100

    def test_purge(self):
        # purge now enforces rules
        # can't purge(1) now. but .purge(now=...) still works
        s = LimitedSet(maxlen=10)
        [s.add(i) for i in range(10)]
        s.maxlen = 2
        s.purge()
        assert len(s) == 2

        # expired
        s = LimitedSet(maxlen=10, expires=1)
        [s.add(i) for i in range(10)]
        s.maxlen = 2
        s.purge(now=monotonic() + 100)
        assert len(s) == 0

        # not expired
        s = LimitedSet(maxlen=None, expires=1)
        [s.add(i) for i in range(10)]
        s.maxlen = 2
        s.purge(now=lambda: monotonic() - 100)
        assert len(s) == 2

        # expired -> minsize
        s = LimitedSet(maxlen=10, minlen=10, expires=1)
        [s.add(i) for i in range(20)]
        s.minlen = 3
        s.purge(now=monotonic() + 3)
        assert s.minlen == len(s)
        assert len(s._heap) <= s.maxlen * (
            100. + s.max_heap_percent_overload) / 100

    def test_pickleable(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.add('bar')
        assert pickle.loads(pickle.dumps(s)) == s

    def test_iter(self):
        s = LimitedSet(maxlen=3)
        items = ['foo', 'bar', 'baz', 'xaz']
        for item in items:
            s.add(item)
        l = list(iter(s))
        for item in items[1:]:
            assert item in l
        assert 'foo' not in l
        assert l == items[1:], 'order by insertion time'

    def test_repr(self):
        s = LimitedSet(maxlen=2)
        items = 'foo', 'bar'
        for item in items:
            s.add(item)
        assert 'LimitedSet(' in repr(s)

    def test_discard(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.discard('foo')
        assert 'foo' not in s
        assert len(s._data) == 0
        s.discard('foo')

    def test_clear(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.add('bar')
        assert len(s) == 2
        s.clear()
        assert not s

    def test_update(self):
        s1 = LimitedSet(maxlen=2)
        s1.add('foo')
        s1.add('bar')

        s2 = LimitedSet(maxlen=2)
        s2.update(s1)
        assert sorted(list(s2)) == ['bar', 'foo']

        s2.update(['bla'])
        assert sorted(list(s2)) == ['bar', 'bla']

        s2.update(['do', 're'])
        assert sorted(list(s2)) == ['do', 're']
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
        assert s3 == s4
        assert s3 == s5
        s2.update(s4)
        s4.update(s2)
        assert s2 == s4

    def test_iterable_and_ordering(self):
        s = LimitedSet(maxlen=35, expires=None)
        clock = count(1)
        for i in reversed(range(15)):
            s.add(i, now=next(clock))
        j = 40
        for i in s:
            assert i < j  # each item is smaller and smaller
            j = i
        assert i == 0  # last item is zero

    def test_pop_and_ordering_again(self):
        s = LimitedSet(maxlen=5)
        for i in range(10):
            s.add(i)
        j = -1
        for _ in range(5):
            i = s.pop()
            assert j < i
        i = s.pop()
        assert i is None

    def test_as_dict(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        assert isinstance(s.as_dict(), Mapping)

    def test_add_removes_duplicate_from_small_heap(self):
        s = LimitedSet(maxlen=2)
        s.add('foo')
        s.add('foo')
        s.add('foo')
        assert len(s) == 1
        assert len(s._data) == 1
        assert len(s._heap) == 1

    def test_add_removes_duplicate_from_big_heap(self):
        s = LimitedSet(maxlen=1000)
        [s.add(i) for i in range(2000)]
        assert len(s) == 1000
        [s.add('foo') for i in range(1000)]
        # heap is refreshed when 15% larger than _data
        assert len(s._heap) < 1150
        [s.add('foo') for i in range(1000)]
        assert len(s._heap) < 1150

    def test_stale_heap_entry_cannot_evict_refreshed_item(self):
        """Test that a stale heap entry cannot evict a refreshed item.

        Regression test for bug where refreshing an item would leave a stale
        heap entry that could later incorrectly remove the current entry.
        Also verifies that the refreshed item eventually expires correctly.
        """
        s = LimitedSet(expires=10)

        # t=1: add item
        s.add("task-id", now=1.0)
        assert "task-id" in s

        # t=6: refresh same item - creates stale heap entry at t=1
        s.add("task-id", now=6.0)
        assert "task-id" in s

        # t=11: purge - old entry expires (t=1 + 10 = 11)
        # The stale entry should NOT remove the current entry (t=6)
        s.purge(now=11.0)
        assert "task-id" in s, "Item should still exist after stale entry expires"

        # t=16: purge - current entry expires (t=6 + 10 = 16)
        s.purge(now=16.0)
        assert "task-id" not in s, "Item should have expired at t=16"

    def test_multiple_refreshes_with_stale_entries(self):
        """Test handling of multiple stale entries from repeated refreshes."""
        s = LimitedSet(expires=10)

        # Multiple refreshes create multiple stale heap entries
        s.add("task-id", now=1.0)
        s.add("task-id", now=3.0)
        s.add("task-id", now=5.0)
        s.add("task-id", now=7.0)

        assert "task-id" in s

        # Purge at t=11 - entries at t=1 and t=3 expire, but current (t=7) should remain
        s.purge(now=11.0)
        assert "task-id" in s, "Item should still exist after old entries expire"

        # Purge at t=17 - current entry (t=7) should now expire
        s.purge(now=17.0)
        assert "task-id" not in s, "Item should have expired at t=17"

    def test_pop_skips_stale_heap_entries(self):
        """Test that pop() skips stale heap entries and only removes current entries."""
        s = LimitedSet(expires=10)

        # Add two items
        s.add("task1", now=1.0)
        s.add("task2", now=2.0)

        # Refresh task1 to create a stale heap entry at t=1
        s.add("task1", now=5.0)

        # Heap now contains: (1, "task1") [stale], (2, "task2"), (5, "task1") [current]
        # Data contains: "task1" -> (5, "task1"), "task2" -> (2, "task2")

        # pop() should skip the stale entry and return task2 (oldest current)
        popped = s.pop()
        assert popped == "task2", f"Expected task2, got {popped}"
        assert "task1" in s, "task1 should still be in set"
        assert "task2" not in s, "task2 should be removed"

        # pop() again should return task1
        popped = s.pop()
        assert popped == "task1", f"Expected task1, got {popped}"
        assert "task1" not in s, "task1 should be removed"

    def test_refresh_with_same_timestamp(self):
        """Test that refreshing an item with the same timestamp creates distinct tuple objects.

        Regression test for bug where == comparison would incorrectly treat stale entries
        as current when timestamps are identical. The invariant must be object identity (is),
        not value equality (==).
        """
        s = LimitedSet(expires=10)

        # Add item with timestamp 5.0
        s.add("task-id", now=5.0)
        entry1 = s._data["task-id"]
        assert len(s._heap) == 1

        # Refresh same item with same timestamp 5.0
        # This creates a NEW tuple object with the SAME values
        s.add("task-id", now=5.0)
        entry2 = s._data["task-id"]

        # Verify they are different objects with equal values
        assert entry1 == entry2, "Values should be equal"
        assert entry1 is not entry2, "Objects should be different"
        assert len(s._heap) == 2, "Heap should contain both entries"

        # purge() should NOT remove the item at t=14.9 (t=5 + 10 = 15)
        # The stale entry (entry1) should be skipped, current entry (entry2) should remain
        s.purge(now=14.9)
        assert "task-id" in s, "Item should still exist at t=14.9 (not yet expired)"

        # At t=15, the item should expire
        s.purge(now=15.0)
        assert "task-id" not in s, "Item should have expired at t=15"


class test_AttributeDict:

    def test_getattr__setattr(self):
        x = AttributeDict({'foo': 'bar'})
        assert x['foo'] == 'bar'
        with pytest.raises(AttributeError):
            x.bar
        x.bar = 'foo'
        assert x['bar'] == 'foo'


class test_Messagebuffer:

    def assert_size_and_first(self, buf, size, expected_first_item):
        assert len(buf) == size
        assert buf.take() == expected_first_item

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
        assert len(b) > 3000
        b.evict()
        assert len(b) == 3000

    def test_pop_empty_with_default(self):
        b = Messagebuffer(10)
        sentinel = object()
        assert b.take(sentinel) is sentinel

    def test_pop_empty_no_default(self):
        b = Messagebuffer(10)
        with pytest.raises(b.Empty):
            b.take()

    def test_repr(self):
        assert repr(Messagebuffer(10, [1, 2, 3]))

    def test_iter(self):
        b = Messagebuffer(10, list(range(10)))
        assert len(b) == 10
        for i, item in enumerate(b):
            assert item == i
        assert len(b) == 0

    def test_contains(self):
        b = Messagebuffer(10, list(range(10)))
        assert 5 in b

    def test_reversed(self):
        assert (list(reversed(Messagebuffer(10, list(range(10))))) ==
                list(reversed(range(10))))

    def test_getitem(self):
        b = Messagebuffer(10, list(range(10)))
        for i in range(10):
            assert b[i] == i


class test_BufferMap:

    def test_append_limited(self):
        b = BufferMap(10)
        for i in range(20):
            b.put(i, i)
        self.assert_size_and_first(b, 10, 10)

    def assert_size_and_first(self, buf, size, expected_first_item):
        assert buf.total == size
        assert buf._LRUpop() == expected_first_item

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
        assert b.take(1, sentinel) is sentinel

    def test_pop_empty_no_default(self):
        b = BufferMap(10)
        with pytest.raises(b.Empty):
            b.take(1)

    def test_repr(self):
        assert repr(Messagebuffer(10, [1, 2, 3]))


class test_ChainMap:

    def test_observers_not_shared(self):
        a = ChainMap()
        b = ChainMap()
        callback = Mock()
        a.bind_to(callback)
        b.update(x=1)
        callback.assert_not_called()
        a.update(x=1)
        callback.assert_called_once_with(x=1)
