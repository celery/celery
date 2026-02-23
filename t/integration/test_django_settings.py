import logging

logging.info("Initializing Django settings for Celery integration tests")

DEBUG = True
INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'django.contrib.auth',
]

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}

CELERY_BROKER_URL = 'memory://'
CELERY_RESULT_BACKEND = 'cache+memory://'
