from __future__ import absolute_import

from . import current_app
from .local import Proxy

tasks = Proxy(lambda: current_app.tasks)
