from celery.loaders.base import BaseLoader


class AppLoader(BaseLoader):

    def on_worker_init(self):
        self.import_default_modules()

    def read_configuration(self):
        return {}
