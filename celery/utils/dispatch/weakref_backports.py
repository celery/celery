"""Weakref compatibility.

weakref_backports is a partial backport of the weakref module for Python
versions below 3.4.

Copyright (C) 2013 Python Software Foundation, see LICENSE.python for details.

The following changes were made to the original sources during backporting:

* Added ``self`` to ``super`` calls.
* Removed ``from None`` when raising exceptions.
"""
from __future__ import absolute_import, unicode_literals
from weakref import ref


class WeakMethod(ref):
    """Weak reference to bound method.

    A custom :class:`weakref.ref` subclass which simulates a weak reference
    to a bound method, working around the lifetime problem of bound methods.
    """

    __slots__ = '_func_ref', '_meth_type', '_alive', '__weakref__'

    def __new__(cls, meth, callback=None):
        try:
            obj = meth.__self__
            func = meth.__func__
        except AttributeError:
            raise TypeError(
                "Argument should be a bound method, not {0}".format(
                    type(meth)))

        def _cb(arg):
            # The self-weakref trick is needed to avoid creating a
            # reference cycle.
            self = self_wr()
            if self._alive:
                self._alive = False
                if callback is not None:
                    callback(self)
        self = ref.__new__(cls, obj, _cb)
        self._func_ref = ref(func, _cb)
        self._meth_type = type(meth)
        self._alive = True
        self_wr = ref(self)
        return self

    def __call__(self):
        obj = super(WeakMethod, self).__call__()
        func = self._func_ref()
        if obj is not None and func is not None:
            return self._meth_type(func, obj)

    def __eq__(self, other):
        if not isinstance(other, WeakMethod):
            return False
        if not self._alive or not other._alive:
            return self is other
        return ref.__eq__(self, other) and self._func_ref == other._func_ref

    def __ne__(self, other):
        if not isinstance(other, WeakMethod):
            return True
        if not self._alive or not other._alive:
            return self is not other
        return ref.__ne__(self, other) or self._func_ref != other._func_ref

    __hash__ = ref.__hash__
