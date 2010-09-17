import os
import sys
import warnings
from importlib import import_module

from celery.datastructures import AttributeDict
from celery.loaders.base import BaseLoader
from celery.exceptions import NotConfigured

DEFAULT_CONFIG_MODULE = "celeryconfig"

DEFAULT_SETTINGS = {
    "DEBUG": False,
    "ADMINS": (),
    "INSTALLED_APPS": ("celery", ),
    "CELERY_IMPORTS": (),
    "CELERY_TASK_ERROR_WHITELIST": (),
}

DEFAULT_UNCONFIGURED_SETTINGS = {
    "CELERY_RESULT_BACKEND": "amqp",
}


def wanted_module_item(item):
    return not item.startswith("_")


class Loader(BaseLoader):
    """The default loader.

    See the FAQ for example usage.

    """

    def setup_settings(self, settingsdict):
        settings = AttributeDict(DEFAULT_SETTINGS, **settingsdict)
        installed_apps = set(list(DEFAULT_SETTINGS["INSTALLED_APPS"]) + \
                             list(settings.INSTALLED_APPS))
        settings.INSTALLED_APPS = tuple(installed_apps)
        settings.CELERY_TASK_ERROR_WHITELIST = tuple(
                getattr(import_module(mod), cls)
                    for fqn in settings.CELERY_TASK_ERROR_WHITELIST
                        for mod, cls in (fqn.rsplit('.', 1), ))

        return settings


    def read_configuration(self):
        """Read configuration from ``celeryconfig.py`` and configure
        celery and Django so it can be used by regular Python."""
        configname = os.environ.get("CELERY_CONFIG_MODULE",
                                    DEFAULT_CONFIG_MODULE)
        try:
            celeryconfig = self.import_from_cwd(configname)
        except ImportError:
            warnings.warn(NotConfigured(
                "No %r module found! Please make sure it exists and "
                "is available to Python." % (configname, )))
            return self.setup_settings(DEFAULT_UNCONFIGURED_SETTINGS)
        else:
            usercfg = dict((key, getattr(celeryconfig, key))
                            for key in dir(celeryconfig)
                                if wanted_module_item(key))
            self.configured = True
            return self.setup_settings(usercfg)

    def on_worker_init(self):
        """Imports modules at worker init so tasks can be registered
        and used by the worked.

        The list of modules to import is taken from the ``CELERY_IMPORTS``
        setting in ``celeryconf.py``.

        """
        self.import_default_modules()
