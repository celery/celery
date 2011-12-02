# -*- coding: utf-8 -*-
"""
    celery.loaders.default
    ~~~~~~~~~~~~~~~~~~~~~~

    The default loader used when no custom app has been initialized.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import os
import warnings

from ..datastructures import AttributeDict
from ..exceptions import NotConfigured
from ..utils import find_module

from .base import BaseLoader

DEFAULT_CONFIG_MODULE = "celeryconfig"


class Loader(BaseLoader):
    """The loader used by the default app."""

    def setup_settings(self, settingsdict):
        return AttributeDict(settingsdict)

    def find_module(self, module):
        return find_module(module)

    def read_configuration(self):
        """Read configuration from :file:`celeryconfig.py` and configure
        celery and Django so it can be used by regular Python."""
        configname = os.environ.get("CELERY_CONFIG_MODULE",
                                     DEFAULT_CONFIG_MODULE)
        try:
            self.find_module(configname)
        except ImportError:
            warnings.warn(NotConfigured(
                "No %r module found! Please make sure it exists and "
                "is available to Python." % (configname, )))
            return self.setup_settings({})
        else:
            celeryconfig = self.import_from_cwd(configname)
            usercfg = dict((key, getattr(celeryconfig, key))
                            for key in dir(celeryconfig)
                                if self.wanted_module_item(key))
            self.configured = True
            return self.setup_settings(usercfg)

    def wanted_module_item(self, item):
        return item[0].isupper() and not item.startswith("_")

    def on_worker_init(self):
        """Imports modules at worker init so tasks can be registered
        and used by the worked.

        The list of modules to import is taken from the
        :setting:`CELERY_IMPORTS` setting.

        """
        self.import_default_modules()
