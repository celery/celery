# -*- coding: utf-8 -*-
"""Object related utilities, including introspection, etc."""
from __future__ import absolute_import, unicode_literals

__all__ = ['Bunch', 'FallbackContext', 'mro_lookup']


class Bunch(object):
    """Object that enables you to modify attributes."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def mro_lookup(cls, attr, stop=set(), monkey_patched=[]):
    """Return the first node by MRO order that defines an attribute.

    Arguments:
        cls (Any): Child class to traverse.
        attr (str): Name of attribute to find.
        stop (Set[Any]): A set of types that if reached will stop
            the search.
        monkey_patched (Sequence): Use one of the stop classes
            if the attributes module origin isn't in this list.
            Used to detect monkey patched attributes.

    Returns:
        Any: The attribute value, or :const:`None` if not found.
    """
    for node in cls.mro():
        if node in stop:
            try:
                value = node.__dict__[attr]
                module_origin = value.__module__
            except (AttributeError, KeyError):
                pass
            else:
                if module_origin not in monkey_patched:
                    return node
            return
        if attr in node.__dict__:
            return node


class FallbackContext(object):
    """Context workaround.

    The built-in ``@contextmanager`` utility does not work well
    when wrapping other contexts, as the traceback is wrong when
    the wrapped context raises.

    This solves this problem and can be used instead of ``@contextmanager``
    in this example::

        @contextmanager
        def connection_or_default_connection(connection=None):
            if connection:
                # user already has a connection, shouldn't close
                # after use
                yield connection
            else:
                # must've new connection, and also close the connection
                # after the block returns
                with create_new_connection() as connection:
                    yield connection

    This wrapper can be used instead for the above like this::

        def connection_or_default_connection(connection=None):
            return FallbackContext(connection, create_new_connection)
    """

    def __init__(self, provided, fallback, *fb_args, **fb_kwargs):
        self.provided = provided
        self.fallback = fallback
        self.fb_args = fb_args
        self.fb_kwargs = fb_kwargs
        self._context = None

    def __enter__(self):
        if self.provided is not None:
            return self.provided
        context = self._context = self.fallback(
            *self.fb_args, **self.fb_kwargs
        ).__enter__()
        return context

    def __exit__(self, *exc_info):
        if self._context is not None:
            return self._context.__exit__(*exc_info)
