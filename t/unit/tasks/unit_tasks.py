from celery import shared_task


@shared_task
def mul(x, y):
    return x * y
