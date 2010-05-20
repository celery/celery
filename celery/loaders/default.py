import os

from celery.loaders.base import BaseLoader

DEFAULT_CONFIG_MODULE = "celeryconfig"

DEFAULT_SETTINGS = {
    "DEBUG": False,
    "ADMINS": (),
    "DATABASE_ENGINE": "sqlite3",
    "DATABASE_NAME": "celery.sqlite",
    "INSTALLED_APPS": ("celery", ),
}


def wanted_module_item(item):
    return not item.startswith("_")


class Settings(dict):

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr_(self, key, value):
        self[key] = value


class Loader(BaseLoader):
    """The default loader.

    See the FAQ for example usage.

    """

    def setup_settings(self, settingsdict):
        settings = Settings(DEFAULT_SETTINGS, **settingsdict)
        installed_apps = set(list(DEFAULT_SETTINGS["INSTALLED_APPS"]) + \
                             list(settings.INSTALLED_APPS))
        settings.INSTALLED_APPS = tuple(installed_apps)

        return settings

    def read_configuration(self):
        """Read configuration from ``celeryconfig.py`` and configure
        celery and Django so it can be used by regular Python."""
        configname = os.environ.get("CELERY_CONFIG_MODULE",
                                    DEFAULT_CONFIG_MODULE)
        celeryconfig = __import__(configname, {}, {}, [''])
        usercfg = dict((key, getattr(celeryconfig, key))
                            for key in dir(celeryconfig)
                                if wanted_module_item(key))
        return self.setup_settings(usercfg)

    def on_worker_init(self):
        """Imports modules at worker init so tasks can be registered
        and used by the worked.

        The list of modules to import is taken from the ``CELERY_IMPORTS``
        setting in ``celeryconf.py``.

        """
        self.import_default_modules()
