from celery.decorators import task
from celery.signals import task_postrun


@task()
def add(x, y):
    return x + y

def sigdump(**kwargs):
    from celery.log import setup_logger
    logger = setup_logger()
    logger.error("Received signal: %s" % repr(kwargs))
task_postrun.connect(sigdump)


@task()
def mul(x, y):
    return x * y
