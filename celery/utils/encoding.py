# -*- coding: utf-8 -*-
"""
    celery.utils.encoding
    ~~~~~~~~~~~~~~~~~~~~~

    Utilities to encode text, and to safely emit text from running
    applications without crashing with the infamous :exc:`UnicodeDecodeError`
    exception.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys
import traceback

is_py3k = sys.version_info >= (3, 0)


if is_py3k:

    def str_to_bytes(s):
        if isinstance(s, str):
            return s.encode()
        return s

    def bytes_to_str(s):
        if isinstance(s, bytes):
            return s.decode()
        return s

    def from_utf8(s, *args, **kwargs):
        return s

    def ensure_bytes(s):
        if not isinstance(s, bytes):
            return str_to_bytes(s)
        return s

    str_t = str
    bytes_t = bytes

else:

    def str_to_bytes(s):                # noqa
        if isinstance(s, unicode):
            return s.encode()
        return s

    def bytes_to_str(s):                # noqa
        return s

    def from_utf8(s, *args, **kwargs):  # noqa
        return s.encode("utf-8", *args, **kwargs)

    str_t = unicode
    bytes_t = str
    ensure_bytes = str_to_bytes


if sys.platform.startswith("java"):

    def default_encoding():
        return "utf-8"
else:

    def default_encoding():       # noqa
        return sys.getfilesystemencoding()


def safe_str(s, errors="replace"):
    s = bytes_to_str(s)
    if not isinstance(s, basestring):
        return safe_repr(s, errors)
    return _safe_str(s, errors)


def _safe_str(s, errors="replace"):
    if is_py3k:
        return s
    encoding = default_encoding()
    try:
        if isinstance(s, unicode):
            return s.encode(encoding, errors)
        return unicode(s, encoding, errors)
    except Exception, exc:
        return "<Unrepresentable %r: %r %r>" % (
                type(s), exc, "\n".join(traceback.format_stack()))


def safe_repr(o, errors="replace"):
    try:
        return repr(o)
    except Exception:
        return _safe_str(o, errors)
