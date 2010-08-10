"""
dfd042c7

SQLAlchemy 0.5.8 version of a805d4bd, see the docstring of that module
for an explanation of this workaround.

"""
from sqlalchemy.types import PickleType as _PickleType
from sqlalchemy import util


class PickleType(_PickleType):

    def process_bind_param(self, value, dialect):
        dumps = self.pickler.dumps
        protocol = self.protocol
        if value is None:
            return None
        return dumps(value, protocol)

    def process_result_value(self, value, dialect):
        loads = self.pickler.loads
        if value is None:
            return None
        return loads(str(value))

    def copy_value(self, value):
        if self.mutable:
            return self.pickler.loads(self.pickler.dumps(value, self.protocol))
        else:
            return value

    def compare_values(self, x, y):
        if self.comparator:
            return self.comparator(x, y)
        elif self.mutable and not hasattr(x, '__eq__') and x is not None:
            util.warn_deprecated(
                    "Objects stored with PickleType when mutable=True "
                    "must implement __eq__() for reliable comparison.")
            a = self.pickler.dumps(x, self.protocol)
            b = self.pickler.dumps(y, self.protocol)
            return a == b
        else:
            return x == y

    def is_mutable(self):
        return self.mutable
