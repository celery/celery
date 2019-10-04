"""Python 2/3 compatibility utilities."""

import sys

import vine.five

sys.modules[__name__] = vine.five
