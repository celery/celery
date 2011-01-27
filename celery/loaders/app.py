from __future__ import absolute_import

import os

from celery.datastructures import DictAttribute
from celery.exceptions import ImproperlyConfigured
from celery.loaders.base import BaseLoader


class AppLoader(BaseLoader):

    def on_worker_init(self):
        self.import_default_modules()

    def read_configuration(self):
        return {}
