from celery.loaders.djangoapp import Loader as DjangoLoader
from celery.loaders.default import Loader as DefaultLoader
from django.conf import settings

Loader = DefaultLoader
if settings.configured:
    Loader = DjangoLoader

current_loader = Loader()
settings = current_loader.conf
