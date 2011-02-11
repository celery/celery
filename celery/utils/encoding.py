import sys


def default_encoding():
    if sys.platform.startswith("java"):
        return "utf-8"
    return sys.getfilesystemencoding()


def safe_str(s, errors="replace"):
    encoding = default_encoding()
    try:
        if isinstance(s, unicode):
            return s.encode(encoding, errors)
        return unicode(s, encoding, errors)
    except Exception, exc:
        return "<Unrepresentable %r: %r>" % (type(s), exc)


def safe_repr(o, errors="replace"):
    try:
        return repr(o)
    except Exception, exc:
        return safe_str(o, errors)
