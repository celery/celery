from tasks import identity, identity_task

from celery import group


def run_example():
    canvas = identity.s("task")
    canvas.link(identity_task.s() | group(identity_task.s(), identity_task.s()))
    canvas.delay()
