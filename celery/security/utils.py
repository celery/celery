# -*- coding: utf-8 -*-
"""
    celery.security.utils
    ~~~~~~~~~~~~~~~~~~~~~

    Utilities used by the message signing serializer.

"""
from __future__ import absolute_import

import sys

from contextlib import contextmanager

from celery.exceptions import SecurityError

try:
    from OpenSSL import crypto
except ImportError:  # pragma: no cover
    crypto = None    # noqa


@contextmanager
def reraise_errors(msg='%r', errors=None):
    assert crypto is not None
    errors = (crypto.Error, ) if errors is None else errors
    try:
        yield
    except errors, exc:
        raise SecurityError, SecurityError(msg % (exc, )), sys.exc_info()[2]
