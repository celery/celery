import sys
import traceback


def default_encoding():
    if sys.platform.startswith("java"):
        return "utf-8"
    return sys.getfilesystemencoding()


def safe_str(s, errors="replace"):
    if not isinstance(s, basestring):
        return safe_repr(s, errors)
    encoding = default_encoding()
    try:
        if isinstance(s, unicode):
            return s.encode(encoding, errors)
        return unicode(s, encoding, errors)
    except Exception:
        return "<Unrepresentable %r: %r>" % (type(s), traceback.format_stack())


def safe_repr(o, errors="replace"):
    try:
        return repr(o)
    except Exception:
        return safe_str(o, errors)
