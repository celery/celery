from celery import task


@task()
def hello_world(to='world'):
    return 'Hello {0}'.format(to)
