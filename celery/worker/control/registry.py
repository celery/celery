# -*- coding: utf-8 -*-
"""
    celery.worker.control.registry
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    The registry keeps track of available remote control commands,
    and can be used to register new commands.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from ...utils.compat import UserDict


class Panel(UserDict):
    data = dict()                               # Global registry.

    @classmethod
    def register(cls, method, name=None):
        cls.data[name or method.__name__] = method
        return method
