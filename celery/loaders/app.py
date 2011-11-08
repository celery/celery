# -*- coding: utf-8 -*-
"""
    celery.loaders.app
    ~~~~~~~~~~~~~~~~~~

    The default loader used with custom app instances.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .base import BaseLoader


class AppLoader(BaseLoader):

    def on_worker_init(self):
        self.import_default_modules()

    def read_configuration(self):
        return {}
