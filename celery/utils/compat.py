try:
    from collections import defaultdict
except ImportError:
    # Written by Jason Kirtland, taken from Python Cookbook:
    # <http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/523034>
    class defaultdict(dict):

        def __init__(self, default_factory=None, *args, **kwargs):
            dict.__init__(self, *args, **kwargs)
            self.default_factory = default_factory

        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)

        def __missing__(self, key):
            if self.default_factory is None:
                raise KeyError(key)
            self[key] = value = self.default_factory()
            return value

        def __reduce__(self):
            f = self.default_factory
            args = f is None and tuple() or f
            return type(self), args, None, None, self.iteritems()

        def copy(self):
            return self.__copy__()

        def __copy__(self):
            return type(self)(self.default_factory, self)

        def __deepcopy__(self):
            import copy
            return type(self)(self.default_factory,
                        copy.deepcopy(self.items()))

        def __repr__(self):
            return "defaultdict(%s, %s)" % (self.default_factory,
                                            dict.__repr__(self))
    import collections
    collections.defaultdict = defaultdict # Pickle needs this.


try:
    all([True])
    all = all
except NameError:
    def all(iterable):
        for item in iterable:
            if not item:
                return False
        return True


try:
    any([True])
    any = any
except NameError:
    def any(iterable):
        for item in iterable:
            if item:
                return True
        return False
