from UserDict import UserDict


class Panel(UserDict):
    data = dict()                       # global registry.

    def __init__(self, logger, listener, hostname=None):
        self.logger = logger
        self.hostname = hostname
        self.listener = listener

    @classmethod
    def register(cls, method, name=None):
        cls.data[name or method.__name__] = method
        return method
