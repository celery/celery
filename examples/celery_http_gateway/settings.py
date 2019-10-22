from __future__ import absolute_import, unicode_literals

import django

# Django settings for celery_http_gateway project.


DEBUG = True
TEMPLATE_DEBUG = DEBUG

CELERY_RESULT_BACKEND = 'database'
BROKER_URL = 'amqp://guest:guest@localhost:5672//'

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

MANAGERS = ADMINS

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'development.db',
        'USER': '',
        'PASSWORD': '',
        'HOST': '',
        'PORT': '',
    }
}

if django.VERSION[:3] < (1, 3):
    DATABASE_ENGINE = DATABASES['default']['ENGINE']
    DATABASE_NAME = DATABASES['default']['NAME']
    DATABASE_USER = DATABASES['default']['USER']
    DATABASE_PASSWORD = DATABASES['default']['PASSWORD']
    DATABASE_HOST = DATABASES['default']['HOST']
    DATABASE_PORT = DATABASES['default']['PORT']

# Local time zone for this installation. Choices can be found here:
# https://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'America/Chicago'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# Absolute path to the directory that holds media.
# Example: '/home/media/media.lawrence.com/'
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there's a path component (optional in other cases).
# Examples: 'http://media.lawrence.com', 'http://example.com/media/'
MEDIA_URL = ''

# URL prefix for admin media -- CSS, JavaScript and images. Make sure to use a
# trailing slash.
# Examples: 'http://foo.com/media/', '/media/'.
ADMIN_MEDIA_PREFIX = '/media/'

# Make this unique, and don't share it with anybody.
# XXX TODO FIXME Set this secret key to anything you want, just change it!
SECRET_KEY = 'This is not a secret, be sure to change this.'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
)

ROOT_URLCONF = 'celery_http_gateway.urls'

TEMPLATE_DIRS = (
    # Put strings here, like '/home/html/django_templates' or
    # 'C:/www/django/templates'.
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'djcelery',
)
