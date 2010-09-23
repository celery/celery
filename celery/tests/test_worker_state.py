import unittest2 as unittest

from celery.datastructures import LimitedSet
from celery.worker import state


class StateResetCase(unittest.TestCase):

    def setUp(self):
        self.reset_state()
        self.on_setup()

    def tearDown(self):
        self.reset_state()
        self.on_teardown()

    def reset_state(self):
        state.active_requests.clear()
        state.revoked.clear()
        state.total_count.clear()

    def on_setup(self):
        pass

    def on_teardown(self):
        pass


class MockShelve(dict):
    filename = None
    in_sync = False
    closed = False

    def open(self, filename):
        self.filename = filename
        return self

    def sync(self):
        self.in_sync = True

    def close(self):
        self.closed = True


class MyPersistent(state.Persistent):
    storage = MockShelve()


class test_Persistent(StateResetCase):

    def on_setup(self):
        self.p = MyPersistent(filename="celery-state")

    def test_constructor(self):
        self.assertDictEqual(self.p.db, {})
        self.assertEqual(self.p.db.filename, self.p.filename)

    def test_save(self):
        self.p.db["foo"] = "bar"
        self.p.save()
        self.assertTrue(self.p.db.in_sync)
        self.assertTrue(self.p.db.closed)

    def add_revoked(self, *ids):
        map(self.p.db.setdefault("revoked", LimitedSet()).add, ids)

    def test_merge(self, data=["foo", "bar", "baz"]):
        self.add_revoked(*data)
        self.p.merge(self.p.db)
        for item in data:
            self.assertIn(item, state.revoked)

    def test_sync(self, data1=["foo", "bar", "baz"],
                        data2=["baz", "ini", "koz"]):
        self.add_revoked(*data1)
        map(state.revoked.add, data2)
        self.p.sync(self.p.db)

        for item in data2:
            self.assertIn(item, self.p.db["revoked"])


class SimpleReq(object):

    def __init__(self, task_name):
        self.task_name = task_name


class test_state(StateResetCase):

    def test_accepted(self, requests=[SimpleReq("foo"),
                                      SimpleReq("bar"),
                                      SimpleReq("baz"),
                                      SimpleReq("baz")]):
        map(state.task_accepted, requests)
        for req in requests:
            self.assertIn(req, state.active_requests)
        self.assertEqual(state.total_count["foo"], 1)
        self.assertEqual(state.total_count["bar"], 1)
        self.assertEqual(state.total_count["baz"], 2)

    def test_ready(self, requests=[SimpleReq("foo"),
                                   SimpleReq("bar")]):
        map(state.task_accepted, requests)
        self.assertEqual(len(state.active_requests), 2)
        map(state.task_ready, requests)
        self.assertEqual(len(state.active_requests), 0)
