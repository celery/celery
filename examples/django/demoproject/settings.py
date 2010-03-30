# Django settings for demoproject project.

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

CELERY_RESULT_BACKEND = "database"
BROKER_HOST = "localhost"
BROKER_PORT = 5672
BROKER_USER = "guest"
BROKER_PASSWORD = "guest"
BROKER_VHOST = "/"

MANAGERS = ADMINS

DATABASE_ENGINE = 'sqlite3'
DATABASE_NAME = 'testdb.sqlite'
DATABASE_USER = ''             # Not used with sqlite3.
DATABASE_PASSWORD = ''         # Not used with sqlite3.
DATABASE_HOST = ''             # Set to empty string for localhost.
                               # Not used with sqlite3.
DATABASE_PORT = ''             # Set to empty string for default.
                               # Not used with sqlite3.

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'celery',
    'demoapp',
    'twitterfollow',
)

TIME_ZONE = 'America/Chicago'
LANGUAGE_CODE = 'en-us'
SITE_ID = 1
USE_I18N = True

# Absolute path to the directory that holds media.
# Example: "/home/media/media.lawrence.com/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there is a path component (optional in other cases).
# Examples: "http://media.lawrence.com", "http://example.com/media/"
MEDIA_URL = ''

# URL prefix for admin media -- CSS, JavaScript and images. Make sure to use a
# trailing slash.
# Examples: "http://foo.com/media/", "/media/".
ADMIN_MEDIA_PREFIX = '/media/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = '!j@j8x^6hr&ue7#n1w@d8zr@#=xqb#br4tjcjy50wuv_5rs7r('

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
#     'django.template.loaders.eggs.load_template_source',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
)

ROOT_URLCONF = 'demoproject.urls'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or
    # "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)
