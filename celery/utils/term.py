
"""

term utils.

>>> colored(red("the quick "),
...         blue("brown ", bold("fox ")),
...         magenta(underline("jumps over")),
...         yellow("the lazy")
...         green("dog"))

"""
from operator import add


BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
OP_SEQ = "\033[%dm"
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"

fold = lambda a: reduce(add, a)

reset = lambda: RESET_SEQ
colored = lambda *s: fold(s) + RESET_SEQ
fg = lambda s: COLOR_SEQ % s
black = lambda *s: fg(30 + BLACK) + fold(s)
red = lambda *s: fg(30 + RED) + fold(s)
green = lambda *s: fg(30 + GREEN) + fold(s)
yellow = lambda *s: fg(30 + YELLOW) + fold(s)
blue = lambda *s: fg(30 + BLUE) + fold(s)
magenta = lambda *s: fg(30 + MAGENTA) + fold(s)
cyan = lambda *s: fg(30 + CYAN) + fold(s)
white = lambda *s: fg(30 + WHITE) + fold(s)
bold = lambda *s: OP_SEQ % 1 + fold(s)
underline = lambda *s: OP_SEQ % 4 + fold(s)
blink = lambda *s: OP_SEQ % 5 + fold(s)
reverse = lambda *s: OP_SEQ % 7 + fold(s)
bright = lambda *s: OP_SEQ % 8 + fold(s)
ired = lambda *s: fg(40 + RED) + fold(s)
igreen = lambda *s: fg(40 + GREEN) + fold(s)
iyellow = lambda *s: fg(40 + YELLOW) + fold(s)
iblue = lambda *s: fg(40 + BLUE) + fold(s)
imagenta = lambda *s: fg(40 + MAGENTA) + fold(s)
icyan = lambda *s: fg(40 + CYAN) + fold(s)
iwhite = lambda *s: fg(40 + WHITE) + fold(s)


