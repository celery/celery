from celery.loaders.base import BaseLoader


class Loader(BaseLoader):

    def read_configuration(self):
        import settings
        from django.core.management import setup_environ
        setup_environ(settings)
        return settings

    def on_worker_init(self):
        imports = getattr(self.conf, "imports", [])
        for module in imports:
            __import__(module, [], [], {''})
