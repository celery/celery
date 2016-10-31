from __future__ import absolute_import, unicode_literals
from celery import task


@task()
def hello_world(to='world'):
    return 'Hello {0}'.format(to)
