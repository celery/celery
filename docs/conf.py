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
