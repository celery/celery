from __future__ import absolute_import, unicode_literals
import pytest
from celery import chain, group, uuid
from celery.exceptions import TimeoutError
from .tasks import add, collect_ids, ids

TIMEOUT = 120


class test_chain:

    def test_simple_chain(self, manager):
        c = add.s(4, 4) | add.s(8) | add.s(16)
        assert c().get(timeout=TIMEOUT) == 32

    def test_complex_chain(self, manager):
        c = (
            add.s(2, 2) | (
                add.s(4) | add.s(8) | add.s(16)
            ) |
            group(add.s(i) for i in range(4))
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [32, 33, 34, 35]

    def test_parent_ids(self, manager, num=10):
        assert manager.inspect().ping()
        c = chain(ids.si(i) for i in range(num))
        c.freeze()
        res = c()
        try:
            res.get(timeout=TIMEOUT)
        except TimeoutError:
            print(manager.inspect.active())
            print(manager.inspect.reserved())
            print(manager.inspect.stats())
            raise
        self.assert_ids(res, num - 1)

    def assert_ids(self, res, size):
        i, root = size, res
        while root.parent:
            root = root.parent
        node = res
        while node:
            root_id, parent_id, value = node.get(timeout=30)
            assert value == i
            assert root_id == root.id
            if node.parent:
                assert parent_id == node.parent.id
            node = node.parent
            i -= 1


class test_group:

    def test_parent_ids(self, manager):
        assert manager.inspect().ping()
        g = ids.si(1) | ids.si(2) | group(ids.si(i) for i in range(2, 50))
        res = g()
        expected_root_id = res.parent.parent.id
        expected_parent_id = res.parent.id
        values = res.get(timeout=TIMEOUT)

        for i, r in enumerate(values):
            root_id, parent_id, value = r
            assert root_id == expected_root_id
            assert parent_id == expected_parent_id
            assert value == i + 2


@pytest.mark.celery(result_backend='redis://')
class xxx_chord:

    def test_parent_ids(self, manager):
        self.assert_parentids_chord()

    def test_parent_ids__already_set(self, manager):
        self.assert_parentids_chord(uuid(), uuid())

    def assert_parentids_chord(self, base_root=None, base_parent=None):
        g = (
            ids.si(1) |
            ids.si(2) |
            group(ids.si(i) for i in range(3, 50)) |
            collect_ids.s(i=50) |
            ids.si(51)
        )
        g.freeze(root_id=base_root, parent_id=base_parent)
        res = g.apply_async(root_id=base_root, parent_id=base_parent)
        expected_root_id = base_root or res.parent.parent.parent.id

        root_id, parent_id, value = res.get(timeout=30)
        assert value == 51
        assert root_id == expected_root_id
        assert parent_id == res.parent.id

        prev, (root_id, parent_id, value) = res.parent.get(timeout=30)
        assert value == 50
        assert root_id == expected_root_id
        assert parent_id == res.parent.parent.id

        for i, p in enumerate(prev):
            root_id, parent_id, value = p
            assert root_id == expected_root_id
            assert parent_id == res.parent.parent.id

        root_id, parent_id, value = res.parent.parent.get(timeout=30)
        assert value == 2
        assert parent_id == res.parent.parent.parent.id
        assert root_id == expected_root_id

        root_id, parent_id, value = res.parent.parent.parent.get(timeout=30)
        assert value == 1
        assert root_id == expected_root_id
        assert parent_id == base_parent
