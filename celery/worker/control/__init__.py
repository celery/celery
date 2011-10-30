# -*- coding: utf-8 -*-
"""
    celery.worker.control
    ~~~~~~~~~~~~~~~~~~~~~

    Remote control commands.
    See :mod:`celery.worker.control.builtins`.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from . import registry

# Loads the built-in remote control commands
from . import builtins  # noqa

Panel = registry.Panel
