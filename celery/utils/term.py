# -*- coding: utf-8 -*-
"""Terminals and colors."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

import base64
=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
import codecs
import base64
import os
import platform
import sys
from functools import reduce
<<<<<<< HEAD

from celery.five import python_2_unicode_compatible, string
=======
from typing import Any, Tuple
from typing import Callable, Mapping  # noqa
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery.platforms import isatty

__all__ = ('colored',)

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
OP_SEQ = '\033[%dm'
RESET_SEQ = '\033[0m'
COLOR_SEQ = '\033[1;%dm'

IS_WINDOWS = platform.system() == 'Windows'

ITERM_PROFILE = os.environ.get('ITERM_PROFILE')
TERM = os.environ.get('TERM')
TERM_IS_SCREEN = TERM and TERM.startswith('screen')

# tmux requires unrecognized OSC sequences to be wrapped with DCS tmux;
# <sequence> ST, and for all ESCs in <sequence> to be replaced with ESC ESC.
# It only accepts ESC backslash for ST.
_IMG_PRE = '\033Ptmux;\033\033]' if TERM_IS_SCREEN else '\033]'
_IMG_POST = '\a\033\\' if TERM_IS_SCREEN else '\a'


def fg(s: int) -> str:
    return COLOR_SEQ % s


class colored:
    """Terminal colored text.

    Example:
        >>> c = colored(enabled=True)
        >>> print(str(c.red('the quick '), c.blue('brown ', c.bold('fox ')),
        ...       c.magenta(c.underline('jumps over')),
        ...       c.yellow(' the lazy '),
        ...       c.green('dog ')))
    """

    def __init__(self, *s: Tuple[Any],
                 enabled: bool=True, op: str = '', **kwargs) -> None:
        self.s = s
        self.enabled = not IS_WINDOWS and enabled
        self.op = op
        self.names = {
            'black': self.black,
            'red': self.red,
            'green': self.green,
            'yellow': self.yellow,
            'blue': self.blue,
            'magenta': self.magenta,
            'cyan': self.cyan,
            'white': self.white,
        }

    def _add(self, a: Any, b: Any) -> str:
        return str(a) + str(b)

    def _fold_no_color(self, a: Any, b: Any) -> str:
        try:
            A = a.no_color()
        except AttributeError:
            A = str(a)
        try:
            B = b.no_color()
        except AttributeError:
            B = str(b)

        return ''.join((str(A), str(B)))

    def no_color(self) -> str:
        if self.s:
            return str(reduce(self._fold_no_color, self.s))
        return ''

    def embed(self) -> str:
        prefix = ''
        if self.enabled:
            prefix = self.op
        return ''.join((str(prefix), str(reduce(self._add, self.s))))

    def node(self, s: Any, op: str) -> Any:
        return self.__class__(enabled=self.enabled, op=op, *s)

    def black(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(30 + BLACK))

    def red(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(30 + RED))

    def green(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(30 + GREEN))

    def yellow(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(30 + YELLOW))

    def blue(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(30 + BLUE))

    def magenta(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(30 + MAGENTA))

    def cyan(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(30 + CYAN))

    def white(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(30 + WHITE))

    def bold(self, *s: Tuple[Any]) -> Any:
        return self.node(s, OP_SEQ % 1)

    def underline(self, *s: Tuple[Any]) -> Any:
        return self.node(s, OP_SEQ % 4)

    def blink(self, *s: Tuple[Any]) -> Any:
        return self.node(s, OP_SEQ % 5)

    def reverse(self, *s: Tuple[Any]) -> Any:
        return self.node(s, OP_SEQ % 7)

    def bright(self, *s: Tuple[Any]) -> Any:
        return self.node(s, OP_SEQ % 8)

    def ired(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(40 + RED))

    def igreen(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(40 + GREEN))

    def iyellow(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(40 + YELLOW))

    def iblue(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(40 + BLUE))

    def imagenta(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(40 + MAGENTA))

    def icyan(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(40 + CYAN))

    def iwhite(self, *s: Tuple[Any]) -> Any:
        return self.node(s, fg(40 + WHITE))

    def reset(self, *s: Tuple[Any]) -> Any:
        return self.node(s or [''], RESET_SEQ)

    def __add__(self, other: Any) -> str:
        return str(self) + str(other)

    def __repr__(self) -> str:
        return repr(self.no_color())

    def __str__(self) -> str:
        suffix = ''
        if self.enabled:
            suffix = RESET_SEQ
        return str(''.join((self.embed(), str(suffix))))


def supports_images() -> bool:
    return isatty(sys.stdin) and ITERM_PROFILE


def _read_as_base64(path: str) -> bytes:
    with codecs.open(path, mode='rb') as fh:
        encoded = base64.b64encode(fh.read())
        return encoded if type(encoded) == 'str' else encoded.decode('ascii')


def imgcat(path: str,
           inline: int = 1,
           preserve_aspect_ratio: int = 0, **kwargs):
    return '\n%s1337;File=inline=%d;preserveAspectRatio=%d:%s%s' % (
        _IMG_PRE, inline, preserve_aspect_ratio,
        _read_as_base64(path), _IMG_POST)
