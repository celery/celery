"""Terminals and colors."""
from __future__ import annotations

import base64
import os
import platform
import sys
from functools import reduce

__all__ = ('colored',)

from typing import Any

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

    def __init__(self, *s: object, **kwargs: Any) -> None:
        self.s: tuple[object, ...] = s
        self.enabled: bool = not IS_WINDOWS and kwargs.get('enabled', True)
        self.op: str = kwargs.get('op', '')
        self.names: dict[str, Any] = {
            'black': self.black,
            'red': self.red,
            'green': self.green,
            'yellow': self.yellow,
            'blue': self.blue,
            'magenta': self.magenta,
            'cyan': self.cyan,
            'white': self.white,
        }

    def _add(self, a: object, b: object) -> str:
        return f"{a}{b}"

    def _fold_no_color(self, a: Any, b: Any) -> str:
        try:
            A = a.no_color()
        except AttributeError:
            A = str(a)
        try:
            B = b.no_color()
        except AttributeError:
            B = str(b)

        return f"{A}{B}"

    def no_color(self) -> str:
        if self.s:
            return str(reduce(self._fold_no_color, self.s))
        return ''

    def embed(self) -> str:
        prefix = ''
        if self.enabled:
            prefix = self.op
        return f"{prefix}{reduce(self._add, self.s)}"

    def __str__(self) -> str:
        suffix = ''
        if self.enabled:
            suffix = RESET_SEQ
        return f"{self.embed()}{suffix}"

    def node(self, s: tuple[object, ...], op: str) -> colored:
        return self.__class__(enabled=self.enabled, op=op, *s)

    def black(self, *s: object) -> colored:
        return self.node(s, fg(30 + BLACK))

    def red(self, *s: object) -> colored:
        return self.node(s, fg(30 + RED))

    def green(self, *s: object) -> colored:
        return self.node(s, fg(30 + GREEN))

    def yellow(self, *s: object) -> colored:
        return self.node(s, fg(30 + YELLOW))

    def blue(self, *s: object) -> colored:
        return self.node(s, fg(30 + BLUE))

    def magenta(self, *s: object) -> colored:
        return self.node(s, fg(30 + MAGENTA))

    def cyan(self, *s: object) -> colored:
        return self.node(s, fg(30 + CYAN))

    def white(self, *s: object) -> colored:
        return self.node(s, fg(30 + WHITE))

    def __repr__(self) -> str:
        return repr(self.no_color())

    def bold(self, *s: object) -> colored:
        return self.node(s, OP_SEQ % 1)

    def underline(self, *s: object) -> colored:
        return self.node(s, OP_SEQ % 4)

    def blink(self, *s: object) -> colored:
        return self.node(s, OP_SEQ % 5)

    def reverse(self, *s: object) -> colored:
        return self.node(s, OP_SEQ % 7)

    def bright(self, *s: object) -> colored:
        return self.node(s, OP_SEQ % 8)

    def ired(self, *s: object) -> colored:
        return self.node(s, fg(40 + RED))

    def igreen(self, *s: object) -> colored:
        return self.node(s, fg(40 + GREEN))

    def iyellow(self, *s: object) -> colored:
        return self.node(s, fg(40 + YELLOW))

    def iblue(self, *s: colored) -> colored:
        return self.node(s, fg(40 + BLUE))

    def imagenta(self, *s: object) -> colored:
        return self.node(s, fg(40 + MAGENTA))

    def icyan(self, *s: object) -> colored:
        return self.node(s, fg(40 + CYAN))

    def iwhite(self, *s: object) -> colored:
        return self.node(s, fg(40 + WHITE))

    def reset(self, *s: object) -> colored:
        return self.node(s or ('',), RESET_SEQ)

    def __add__(self, other: object) -> str:
        return f"{self}{other}"


def supports_images() -> bool:

    try:
        return sys.stdin.isatty() and bool(os.environ.get('ITERM_PROFILE'))
    except AttributeError:
        return False


def _read_as_base64(path: str) -> str:
    with open(path, mode='rb') as fh:
        encoded = base64.b64encode(fh.read())
        return encoded.decode('ascii')


def imgcat(path: str, inline: int = 1, preserve_aspect_ratio: int = 0, **kwargs: Any) -> str:
    return '\n%s1337;File=inline=%d;preserveAspectRatio=%d:%s%s' % (
        _IMG_PRE, inline, preserve_aspect_ratio,
        _read_as_base64(path), _IMG_POST)
