# -*- coding: utf-8 -*-
"""
    celery.utils.encoding
    ~~~~~~~~~~~~~~~~~~~~~

    This module has moved to :mod:`kombu.utils.encoding`.

"""
from __future__ import absolute_import, unicode_literals

from kombu.utils.encoding import (  # noqa
    default_encode, default_encoding, bytes_t, bytes_to_str, str_t,
    str_to_bytes, ensure_bytes, from_utf8, safe_str, safe_repr,
)
