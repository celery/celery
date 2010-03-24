import os

from celery.loaders.base import BaseLoader

DEFAULT_CONFIG_MODULE = "celeryconfig"

DEFAULT_SETTINGS = {
    "DEBUG": False,
    "DATABASE_ENGINE": "sqlite3",
    "DATABASE_NAME": "celery.sqlite",
    "INSTALLED_APPS": ("celery", ),
}


def wanted_module_item(item):
    return not item.startswith("_")


class Loader(BaseLoader):
    """The default loader.

    See the FAQ for example usage.

    """

    def setup_django_env(self, settingsdict):
        print("SETUP_DJANGO_ENV")
        config = dict(DEFAULT_SETTINGS, **settingsdict)

        from django.conf import settings
        if not settings.configured:
            settings.configure()
        for config_key, config_value in config.items():
            setattr(settings, config_key, config_value)
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
        return self.setup_django_env(usercfg)

    def on_worker_init(self):
        """Imports modules at worker init so tasks can be registered
        and used by the worked.

        The list of modules to import is taken from the ``CELERY_IMPORTS``
        setting in ``celeryconf.py``.

        """
        self.import_default_modules()
