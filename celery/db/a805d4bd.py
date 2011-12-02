# -*- coding: utf-8 -*-
"""
a805d4bd
This module fixes a bug with pickling and relative imports in Python < 2.6.

The problem is with pickling an e.g. `exceptions.KeyError` instance.
As SQLAlchemy has its own `exceptions` module, pickle will try to
lookup :exc:`KeyError` in the wrong module, resulting in this exception::

    cPickle.PicklingError: Can't pickle <type 'exceptions.KeyError'>:
        attribute lookup exceptions.KeyError failed

doing `import exceptions` just before the dump in `sqlalchemy.types`
reveals the source of the bug::

    EXCEPTIONS: <module 'sqlalchemy.exc' from '/var/lib/hudson/jobs/celery/
        workspace/buildenv/lib/python2.5/site-packages/sqlalchemy/exc.pyc'>

Hence the random module name "a805d5bd" is taken to decrease the chances of
a collision.

"""
from __future__ import absolute_import

from sqlalchemy.types import PickleType as _PickleType


class PickleType(_PickleType):

    def bind_processor(self, dialect):
        impl_processor = self.impl.bind_processor(dialect)
        dumps = self.pickler.dumps
        protocol = self.protocol
        if impl_processor:

            def process(value):
                if value is not None:
                    value = dumps(value, protocol)
                return impl_processor(value)

        else:

            def process(value):  # noqa
                if value is not None:
                    value = dumps(value, protocol)
                return value
        return process

    def result_processor(self, dialect, coltype):
        impl_processor = self.impl.result_processor(dialect, coltype)
        loads = self.pickler.loads
        if impl_processor:

            def process(value):
                value = impl_processor(value)
                if value is None:
                    return None
                return loads(value)
        else:

            def process(value):  # noqa
                if value is None:
                    return None
                return loads(value)
        return process

    def copy_value(self, value):
        if self.mutable:
            return self.pickler.loads(self.pickler.dumps(value, self.protocol))
        else:
            return value
