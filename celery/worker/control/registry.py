from __future__ import absolute_import

from ...utils.compat import UserDict


class Panel(UserDict):
    data = dict()                               # Global registry.

    @classmethod
    def register(cls, method, name=None):
        cls.data[name or method.__name__] = method
        return method
