# -*- coding: utf-8 -*-
"""The default loader used with custom app instances."""
from __future__ import absolute_import, unicode_literals

from .base import BaseLoader

__all__ = ('AppLoader',)


class AppLoader(BaseLoader):
    """Default loader used when an app is specified."""
