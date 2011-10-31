"""

celery.loaders.app
==================

The default loader used with custom app instances.

"""
from __future__ import absolute_import

from .base import BaseLoader

__all__ = ["AppLoader"]


class AppLoader(BaseLoader):

    def on_worker_init(self):
        self.import_default_modules()

    def read_configuration(self):
        return {}
