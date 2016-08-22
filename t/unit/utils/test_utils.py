from __future__ import absolute_import, unicode_literals

import pytest

from celery.utils import chunks, cached_property


@pytest.mark.parametrize('items,n,expected', [
    (range(11), 2, [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]]),
    (range(11), 3, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]),
    (range(10), 2, [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]),
])
def test_chunks(items, n, expected):
        x = chunks(iter(list(items)), n)
        assert list(x) == expected


def test_cached_property():

    def fun(obj):
        return fun.value

    x = cached_property(fun)
    assert x.__get__(None) is x
    assert x.__set__(None, None) is x
    assert x.__delete__(None) is x
