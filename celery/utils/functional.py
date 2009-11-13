"""Functional utilities for Python 2.4 compatibility."""


def _compat_curry(fun, *args, **kwargs):
    """New function with partial application of the given arguments
    and keywords."""

    def _curried(*addargs, **addkwargs):
        return fun(*(args+addargs), **dict(kwargs, **addkwargs))
    return _curried


try:
    from functools import partial as curry
except ImportError:
    curry = _compat_curry
