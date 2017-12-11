# -*- coding: utf-8 -*-
"""Python 2/3 compatibility utilities."""
from __future__ import absolute_import, unicode_literals

import sys

import vine.five

sys.modules[__name__] = vine.five
