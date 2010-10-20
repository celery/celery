from UserDict import UserDict

from celery.app import app_or_default


class Panel(UserDict):
    data = dict()                               # Global registry.

    def __init__(self, logger, consumer, hostname=None, app=None):
        self.app = app_or_default(app)
        self.logger = logger
        self.hostname = hostname
        self.consumer = consumer
        # Compat (pre 2.2)
        self.listener = consumer

    @classmethod
    def register(cls, method, name=None):
        cls.data[name or method.__name__] = method
        return method
