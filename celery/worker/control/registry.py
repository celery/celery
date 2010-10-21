from UserDict import UserDict

from celery.app import app_or_default


class Panel(UserDict):
    data = dict()                               # Global registry.

    @classmethod
    def register(cls, method, name=None):
        cls.data[name or method.__name__] = method
        return method
