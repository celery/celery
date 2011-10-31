"""

celery.worker.control
=====================

Remote control commands.
See :mod:`celery.worker.control.builtins`.

"""
from __future__ import absolute_import

from . import registry

# Loads the built-in remote control commands
from . import builtins  # noqa

Panel = registry.Panel
