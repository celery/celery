# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from sphinx_celery import conf

globals().update(conf.build_config(
    'celery', __file__,
    project='Celery',
    version_dev='5.0',
    version_stable='4.0',
    canonical_url='http://docs.celeryproject.org',
    webdomain='celeryproject.org',
    github_project='celery/celery',
    author='Ask Solem & contributors',
    author_name='Ask Solem',
    copyright='2009-2018',
    publisher='Celery Project',
    html_logo='images/celery_512.png',
    html_favicon='images/favicon.ico',
    html_prepend_sidebars=['sidebardonations.html'],
    extra_extensions=[
        'sphinx.ext.napoleon',
        'celery.contrib.sphinx',
        'celerydocs',
    ],
    extra_intersphinx_mapping={
        'cyanide': ('https://cyanide.readthedocs.io/en/latest', None),
    },
    apicheck_ignore_modules=[
        'celery.five',
        'celery.__main__',
        'celery.task',
        'celery.contrib.testing',
        'celery.contrib.testing.tasks',
        'celery.task.base',
        'celery.bin',
        'celery.bin.celeryd_detach',
        'celery.contrib',
        r'celery.fixups.*',
        'celery.local',
        'celery.app.base',
        'celery.apps',
        'celery.canvas',
        'celery.concurrency.asynpool',
        'celery.utils.encoding',
        r'celery.utils.static.*',
    ],
    linkcheck_ignore=[
        r'^http://localhost'
    ]
))

settings = {}
ignored_settings = {
    # Deprecated broker settings (replaced by broker_url)
    'broker_host',
    'broker_user',
    'broker_password',
    'broker_vhost',
    'broker_port',
    'broker_transport',

    # deprecated task settings.
    'chord_propagates',

    # MongoDB settings replaced by URL config.,
    'mongodb_backend_settings',

    # Database URL replaced by URL config (result_backend = db+...).
    'database_url',

    # Redis settings replaced by URL config.
    'redis_host',
    'redis_port',
    'redis_db',
    'redis_password',

    # Old deprecated AMQP result backend.
    'result_exchange',
    'result_exchange_type',

    # Experimental
    'worker_agent',

    # Deprecated worker settings.
    'worker_pool_putlocks',
}


def configcheck_project_settings():
    from celery.app.defaults import NAMESPACES, flatten
    settings.update(dict(flatten(NAMESPACES)))
    return set(settings)


def is_deprecated_setting(setting):
    try:
        return settings[setting].deprecate_by
    except KeyError:
        pass


def configcheck_should_ignore(setting):
    return setting in ignored_settings or is_deprecated_setting(setting)
