# -*- coding: utf-8 -*-
"""Text formatting utilities."""
import re

from collections import Callable
from functools import partial
from pprint import pformat
from textwrap import fill
from typing import Any, ByteString, Mapping, Pattern, Sequence, Union


__all__ = [
    'abbr', 'abbrtask', 'dedent', 'dedent_initial',
    'ensure_newlines', 'ensure_sep',
    'fill_paragraphs', 'indent', 'join',
    'pluralize', 'pretty', 'str_to_list', 'simple_format', 'truncate',
]

UNKNOWN_SIMPLE_FORMAT_KEY = """
Unknown format %{0} in string {1!r}.
Possible causes: Did you forget to escape the expand sign (use '%%{0!r}'),
or did you escape and the value was expanded twice? (%%N -> %N -> %hostname)?
""".strip()

RE_FORMAT = re.compile(r'%(\w)')


def str_to_list(s: Union[str, Sequence[str]]) -> Sequence[str]:
    """Parse comma separated string into list."""
    if isinstance(s, str):
        return s.split(',')
    return s


def dedent_initial(s: str, n: int=4) -> str:
    """Remove identation from first line of text."""
    return s[n:] if s[:n] == ' ' * n else s


def dedent(s: str, n: int=4, sep: str='\n') -> str:
    """Remove identation."""
    return sep.join(dedent_initial(l) for l in s.splitlines())


def fill_paragraphs(s: str, width: int, sep: str='\n') -> str:
    """Fill paragraphs with newlines (or custom separator)."""
    return sep.join(fill(p, width) for p in s.split(sep))


def join(l: Sequence[str], sep: str='\n') -> str:
    """Concatenate list of strings."""
    return sep.join(v for v in l if v)


def ensure_sep(sep: str, s: str, n: int=2) -> str:
    """Ensure text s ends in separator sep'."""
    return s + sep * (n - s.count(sep))
ensure_newlines = partial(ensure_sep, '\n')


def abbr(S: str, max: int, ellipsis: str='...') -> str:
    """Abbreviate word."""
    if S is None:
        return '???'
    if len(S) > max:
        return ellipsis and (S[:max - len(ellipsis)] + ellipsis) or S[:max]
    return S


def abbrtask(S: str, max: int) -> str:
    """Abbreviate task name."""
    if S is None:
        return '???'
    if len(S) > max:
        module, _, cls = S.rpartition('.')
        module = abbr(module, max - len(cls) - 3, False)
        return module + '[.]' + cls
    return S


def indent(t: str, indent: int=0, sep: str='\n') -> str:
    """Indent text."""
    return sep.join(' ' * indent + p for p in t.split(sep))


def truncate(s: str, maxlen: int=128, suffix: str='...') -> str:
    """Truncate text to a maximum number of characters."""
    if maxlen and len(s) >= maxlen:
        return s[:maxlen].rsplit(' ', 1)[0] + suffix
    return s


def truncate_bytes(s: ByteString,
                   maxlen: int=128, suffix: ByteString=b'...') -> ByteString:
    if maxlen and len(s) >= maxlen:
        return s[:maxlen].rsplit(b' ', 1)[0] + suffix
    return s


def pluralize(n: int, text: str, suffix: str='s') -> str:
    """Pluralize term when n is greater than one."""
    if n != 1:
        return text + suffix
    return text


def pretty(value: Any,
           width: int=80, nl_width: int=80, sep: str='\n', **kw) -> str:
    """Format value for printing to console."""
    if isinstance(value, dict):
        return '{{{0} {1}'.format(sep, pformat(value, 4, nl_width)[1:])
    elif isinstance(value, tuple):
        return '{0}{1}{2}'.format(
            sep, ' ' * 4, pformat(value, width=nl_width, **kw),
        )
    else:
        return pformat(value, width=width, **kw)


def match_case(s: str, other: str) -> str:
    return s.upper() if other.isupper() else s.lower()


def simple_format(s: str, keys: Mapping[str, Any],
                  pattern: Pattern=RE_FORMAT, expand: Pattern=r'\1') -> str:
    """Format string, expanding abbreviations in keys'."""
    if s:
        keys.setdefault('%', '%')

        def resolve(match):
            key = match.expand(expand)
            try:
                resolver = keys[key]
            except KeyError:
                raise ValueError(UNKNOWN_SIMPLE_FORMAT_KEY.format(key, s))
            if isinstance(resolver, Callable):
                return resolver()
            return resolver

        return pattern.sub(resolve, s)
    return s
