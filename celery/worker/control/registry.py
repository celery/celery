"""

celery.worker.control.registry
==============================

The registry keeps track of available remote control commands,
and can be used to register new commands.

"""
from __future__ import absolute_import

from ...utils.compat import UserDict

__all__ = ["Panel"]


class Panel(UserDict):
    data = dict()                               # Global registry.

    @classmethod
    def register(cls, method, name=None):
        cls.data[name or method.__name__] = method
        return method
