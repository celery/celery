from celery.loaders.base import BaseLoader

DEFAULT_SETTINGS = {
    "DEBUG": False,
    "DATABASE_ENGINE": "sqlite3",
    "DATABASE_NAME": "celery.sqlite",
    "INSTALLED_APPS": ("celery", ),
}


def wanted_module_item(item):
    is_private = item.startswith("_")
    return not is_private


class Loader(BaseLoader):

    def read_configuration(self):
        config = dict(DEFAULT_SETTINGS)
        try:
            import celeryconfig
        except ImportError:
            pass
        else:
            usercfg = dict((key, getattr(celeryconfig, key))
                            for key in dir(celeryconfig)
                                if wanted_module_item(key))
        config.update(usercfg)
        from django.conf import settings
        if not settings.configured:
            settings.configure(**config)
        return settings

    def on_worker_init(self):
        imports = getattr(self.conf, "CELERY_TASK_MODULES", [])
        for module in imports:
            __import__(module, [], [], [''])
