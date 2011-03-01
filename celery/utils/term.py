"""

term utils.

>>> c = colored(enabled=True)
>>> print(str(c.red("the quick "), c.blue("brown ", c.bold("fox ")),
              c.magenta(c.underline("jumps over")),
              c.yellow(" the lazy "),
              c.green("dog ")))

"""
import platform

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
OP_SEQ = "\033[%dm"
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
fg = lambda s: COLOR_SEQ % s

SYSTEM = platform.system()
IS_WINDOWS = SYSTEM == "Windows"


class colored(object):

    def __init__(self, *s, **kwargs):
        self.s = s
        self.enabled = kwargs.get("enabled", True)
        if IS_WINDOWS:
            self.enabled = False
        self.op = kwargs.get("op", "")
        self.names = {"black": self.black,
                      "red": self.red,
                      "green": self.green,
                      "yellow": self.yellow,
                      "blue": self.blue,
                      "magenta": self.magenta,
                      "cyan": self.cyan,
                      "white": self.white}

    def _add(self, a, b):
        return str(a) + str(b)

    def _fold_no_color(self, a, b):
        try:
            A = a.no_color()
        except AttributeError:
            A = str(a)
        try:
            B = b.no_color()
        except AttributeError:
            B = str(b)
        return A + B

    def no_color(self):
        return reduce(self._fold_no_color, self.s)

    def __str__(self):
        prefix, suffix = "", ""
        if self.enabled:
            prefix, suffix = self.op, RESET_SEQ
        return prefix + str(reduce(self._add, self.s)) + suffix

    def node(self, s, op):
        return self.__class__(enabled=self.enabled, op=op, *s)

    def black(self, *s):
        return self.node(s, fg(30 + BLACK))

    def red(self, *s):
        return self.node(s, fg(30 + RED))

    def green(self, *s):
        return self.node(s, fg(30 + GREEN))

    def yellow(self, *s):
        return self.node(s, fg(30 + YELLOW))

    def blue(self, *s):
        return self.node(s, fg(30 + BLUE))

    def magenta(self, *s):
        return self.node(s, fg(30 + MAGENTA))

    def cyan(self, *s):
        return self.node(s, fg(30 + CYAN))

    def white(self, *s):
        return self.node(s, fg(30 + WHITE))

    def __repr__(self):
        return repr(self.no_color())

    def bold(self, *s):
        return self.node(s, OP_SEQ % 1)

    def underline(self, *s):
        return self.node(s, OP_SEQ % 4)

    def blink(self, *s):
        return self.node(s, OP_SEQ % 5)

    def reverse(self, *s):
        return self.node(s, OP_SEQ % 7)

    def bright(self, *s):
        return self.node(s, OP_SEQ % 8)

    def ired(self, *s):
        return self.node(s, fg(40 + RED))

    def igreen(self, *s):
        return self.node(s, fg(40 + GREEN))

    def iyellow(self, *s):
        return self.node(s, fg(40 + YELLOW))

    def iblue(self, *s):
        return self.node(s, fg(40, BLUE))

    def imagenta(self, *s):
        return self.node(s, fg(40 + MAGENTA))

    def icyan(self, *s):
        return self.node(s, fg(40 + CYAN))

    def iwhite(self, *s):
        return self.node(s, fg(40 + WHITE))

    def reset(self, *s):
        return self.node(s, RESET_SEQ)

    def __add__(self, other):
        return str(self) + str(other)
