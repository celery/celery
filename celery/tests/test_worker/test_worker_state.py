from __future__ import absolute_import

from celery.datastructures import LimitedSet
from celery.worker import state
from celery.tests.utils import Case


class StateResetCase(Case):

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

    def open(self, filename, **kwargs):
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
        for id in ids:
            self.p.db.setdefault("revoked", LimitedSet()).add(id)

    def test_merge(self, data=["foo", "bar", "baz"]):
        self.add_revoked(*data)
        self.p.merge(self.p.db)
        for item in data:
            self.assertIn(item, state.revoked)

    def test_sync(self, data1=["foo", "bar", "baz"],
                        data2=["baz", "ini", "koz"]):
        self.add_revoked(*data1)
        for item in data2:
            state.revoked.add(item)
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
        for request in requests:
            state.task_accepted(request)
        for req in requests:
            self.assertIn(req, state.active_requests)
        self.assertEqual(state.total_count["foo"], 1)
        self.assertEqual(state.total_count["bar"], 1)
        self.assertEqual(state.total_count["baz"], 2)

    def test_ready(self, requests=[SimpleReq("foo"),
                                   SimpleReq("bar")]):
        for request in requests:
            state.task_accepted(request)
        self.assertEqual(len(state.active_requests), 2)
        for request in requests:
            state.task_ready(request)
        self.assertEqual(len(state.active_requests), 0)
