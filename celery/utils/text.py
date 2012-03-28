from __future__ import absolute_import

from pprint import pformat


def abbr(S, max, ellipsis="..."):
    if S is None:
        return "???"
    if len(S) > max:
        return ellipsis and (S[:max - len(ellipsis)] + ellipsis) or S[:max]
    return S


def abbrtask(S, max):
    if S is None:
        return "???"
    if len(S) > max:
        module, _, cls = S.rpartition(".")
        module = abbr(module, max - len(cls) - 3, False)
        return module + "[.]" + cls
    return S


def indent(t, indent=0):
        """Indent text."""
        return "\n".join(" " * indent + p for p in t.split("\n"))


def truncate(text, maxlen=128, suffix='...'):
    """Truncates text to a maximum number of characters."""
    if len(text) >= maxlen:
        return text[:maxlen].rsplit(" ", 1)[0] + suffix
    return text


def pluralize(n, text, suffix='s'):
    if n > 1:
        return text + suffix
    return text


def pretty(value, width=80, nl_width=80, **kw):
    if isinstance(value, dict):
        return "{\n %s" % (pformat(value, 4, nl_width)[1:])
    elif isinstance(value, tuple):
        return "\n%s%s" % (' ' * 4, pformat(value, width=nl_width, **kw))
    else:
        return pformat(value, width=width, **kw)
