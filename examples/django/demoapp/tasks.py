# Create your tasks here

from demoapp.models import Widget

from celery import shared_task


@shared_task
def add(x, y):
    return x + y


@shared_task
def mul(x, y):
    return x * y


@shared_task
def xsum(numbers):
    return sum(numbers)


@shared_task
def count_widgets():
    return Widget.objects.count()


@shared_task
def rename_widget(widget_id, name):
    w = Widget.objects.get(id=widget_id)
    w.name = name
    w.save()
    
@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 10 * 60},  # retry 2 times in 10 minutes
)
def error_task():
    raise Exception("Test error")

@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=5,  # Factor in seconds (first retry: 5s, second: 10s, third: 20s, etc.)
    retry_jitter=False,  # Set False to disable randomization (use exact values: 5s, 10s, 20s)
    retry_kwargs={"max_retries": 3},
)
def error_backoff_test(self):
    raise Exception("Test error")
    return "Success"
