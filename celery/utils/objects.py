# -*- coding: utf-8 -*-
"""
    celery.utils.objects
    ~~~~~~~~~~~~~~~~~~~~

    Object related utilities including introspection, etc.

"""
from __future__ import absolute_import

__all__ = ['mro_lookup']


def mro_lookup(cls, attr, stop=(), monkey_patched=[]):
    """Return the first node by MRO order that defines an attribute.

    :keyword stop: A list of types that if reached will stop the search.
    :keyword monkey_patched: Use one of the stop classes if the attr's
        module origin is not in this list, this to detect monkey patched
        attributes.

    :returns None: if the attribute was not found.

    """
    for node in cls.mro():
        if node in stop:
            try:
                attr = node.__dict__[attr]
                module_origin = attr.__module__
            except (AttributeError, KeyError):
                pass
            else:
                if module_origin not in monkey_patched:
                    return node
            return
        if attr in node.__dict__:
            return node
