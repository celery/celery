from UserDict import UserDict


class Panel(UserDict):
    data = dict() # Global registry.

    def __init__(self, logger, listener, hostname=None):
        self.logger = logger
        self.hostname = hostname
        self.listener = listener

    @classmethod
    def register(cls, method, name=None):
        cls.data[name or method.__name__] = method
        return method

    @classmethod
    def unregister(cls, name_or_method):
        name = name_or_method
        if not isinstance(name_or_method, basestring):
            name = name_or_method.__name__
        cls.data.pop(name)
