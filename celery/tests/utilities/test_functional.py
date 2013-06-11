from __future__ import absolute_import

import pickle

from celery.five import THREAD_TIMEOUT_MAX, items, range
from celery.utils.functional import LRUCache

from celery.tests.utils import Case


class test_LRUCache(Case):

    def test_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x[i] = i
        self.assertListEqual(list(x.keys()), list(slots[limit:]))
        self.assertTrue(x.items())
        self.assertTrue(x.values())

    def test_is_pickleable(self):
        x = LRUCache(limit=10)
        x.update(luke=1, leia=2)
        y = pickle.loads(pickle.dumps(x))
        self.assertEqual(y.limit, y.limit)
        self.assertEqual(y, x)

    def test_update_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x.update({i: i})

        self.assertListEqual(list(x.keys()), list(slots[limit:]))

    def test_least_recently_used(self):
        x = LRUCache(3)

        x[1], x[2], x[3] = 1, 2, 3
        self.assertEqual(list(x.keys()), [1, 2, 3])

        x[4], x[5] = 4, 5
        self.assertEqual(list(x.keys()), [3, 4, 5])

        # access 3, which makes it the last used key.
        x[3]
        x[6] = 6
        self.assertEqual(list(x.keys()), [5, 3, 6])

        x[7] = 7
        self.assertEqual(list(x.keys()), [3, 6, 7])

    def assertSafeIter(self, method, interval=0.01, size=10000):
        from threading import Thread, Event
        from time import sleep
        x = LRUCache(size)
        x.update(zip(range(size), range(size)))

        class Burglar(Thread):

            def __init__(self, cache):
                self.cache = cache
                self._is_shutdown = Event()
                self._is_stopped = Event()
                Thread.__init__(self)

            def run(self):
                while not self._is_shutdown.isSet():
                    try:
                        self.cache.data.popitem(last=False)
                    except KeyError:
                        break
                self._is_stopped.set()

            def stop(self):
                self._is_shutdown.set()
                self._is_stopped.wait()
                self.join(THREAD_TIMEOUT_MAX)

        burglar = Burglar(x)
        burglar.start()
        try:
            for _ in getattr(x, method)():
                sleep(0.0001)
        finally:
            burglar.stop()

    def test_safe_to_remove_while_iteritems(self):
        self.assertSafeIter('iteritems')

    def test_safe_to_remove_while_keys(self):
        self.assertSafeIter('keys')

    def test_safe_to_remove_while_itervalues(self):
        self.assertSafeIter('itervalues')

    def test_items(self):
        c = LRUCache()
        c.update(a=1, b=2, c=3)
        self.assertTrue(list(items(c)))
