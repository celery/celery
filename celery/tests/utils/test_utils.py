from __future__ import absolute_import, unicode_literals

from celery.utils import chunks, cached_property

from celery.tests.case import Case


class test_chunks(Case):

    def test_chunks(self):

        # n == 2
        x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2)
        self.assertListEqual(
            list(x),
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]],
        )

        # n == 3
        x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 3)
        self.assertListEqual(
            list(x),
            [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]],
        )

        # n == 2 (exact)
        x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 2)
        self.assertListEqual(
            list(x),
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]],
        )


class test_utils(Case):

    def test_cached_property(self):

        def fun(obj):
            return fun.value

        x = cached_property(fun)
        self.assertIs(x.__get__(None), x)
        self.assertIs(x.__set__(None, None), x)
        self.assertIs(x.__delete__(None), x)
