from celery import task


@task()
def hello_world(to='world'):
    return f'Hello {to}'
