# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from sphinx_celery import conf

globals().update(conf.build_config(
    'celery', __file__,
    project='Celery',
    version_dev='4.0',
    version_stable='3.1',
    canonical_url='http://docs.celeryproject.org',
    webdomain='celeryproject.org',
    github_project='celery/celery',
    author='Ask Solem & contributors',
    author_name='Ask Solem',
    copyright='2009-2016',
    publisher='Celery Project',
    html_logo='images/celery_128.png',
    html_favicon='images/favicon.ico',
    html_prepend_sidebars=['sidebardonations.html'],
    extra_extensions=[
        'celery.contrib.sphinx',
        'celerydocs',
    ],
    apicheck_ignore_modules=[
        'celery.five',
        'celery.__main__',
        'celery.task',
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
    ],
))

settings = {}
ignored_settings = {
    'worker_agent',
    'worker_pool_putlocks',
    'broker_host',
    'broker_user',
    'broker_password',
    'broker_vhost',
    'broker_port',
    'broker_transport',
    'chord_propagates',
    'redis_host',
    'redis_port',
    'redis_db',
    'redis_password',
    'worker_force_execv',
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
