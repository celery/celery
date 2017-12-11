# -*- coding: utf-8 -*-
"""Utilities used by the message signing serializer."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
import sys
from contextlib import contextmanager

from celery.exceptions import SecurityError
<<<<<<< HEAD
from celery.five import reraise

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
try:
    from OpenSSL import crypto
except ImportError:  # pragma: no cover
    crypto = None    # noqa

__all__ = ('reraise_errors',)


@contextmanager
def reraise_errors(msg='{0!r}', errors=None):
    """Context reraising crypto errors as :exc:`SecurityError`."""
    assert crypto is not None
    errors = (crypto.Error,) if errors is None else errors
    try:
        yield
    except errors as exc:
        raise SecurityError(msg.format(exc)).with_traceback(sys.exc_info()[2])
