# -*- coding: utf-8 -*-
"""
    celery.loaders.default
    ~~~~~~~~~~~~~~~~~~~~~~

    The default loader used when no custom app has been initialized.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import os
import sys
import warnings

from celery.datastructures import AttributeDict
from celery.exceptions import NotConfigured
from celery.utils import strtobool
from celery.utils.imports import NotAPackage, find_module

from .base import BaseLoader

DEFAULT_CONFIG_MODULE = "celeryconfig"

#: Warns if configuration file is missing if :envvar:`C_WNOCONF` is set.
C_WNOCONF = strtobool(os.environ.get("C_WNOCONF", False))

CONFIG_INVALID_NAME = """
Error: Module '%(module)s' doesn't exist, or it's not a valid \
Python module name.
"""

CONFIG_WITH_SUFFIX = CONFIG_INVALID_NAME + """
Did you mean '%(suggest)s'?
"""


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
        except NotAPackage:
            if configname.endswith('.py'):
                raise NotAPackage, NotAPackage(
                        CONFIG_WITH_SUFFIX % {
                            "module": configname,
                            "suggest": configname[:-3]}), sys.exc_info()[2]
            raise NotAPackage, NotAPackage(
                    CONFIG_INVALID_NAME % {
                        "module": configname}), sys.exc_info()[2]
        except ImportError:
            # billiard sets this if forked using execv
            if C_WNOCONF and not os.environ.get("FORKED_BY_MULTIPROCESSING"):
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
