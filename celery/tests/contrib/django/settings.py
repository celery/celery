# -*- encoding: utf-8 -*-
from __future__ import unicode_literals
import warnings

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}

warnings.simplefilter('ignore', Warning)

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'celery.tests.contrib.django.testapp'
)

SITE_ID = 1
ROOT_URLCONF = 'core.urls'

SECRET_KEY = 'foobar'

USE_L10N = True

LOGGING = {
    'version': 1,
    'loggers': {
        'django.db.backends.schema': {
            'level': 'INFO',
        },
    }
}