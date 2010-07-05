from celery.utils.compat import defaultdict

active = defaultdict(lambda: 0)
total = defaultdict(lambda: 0)


def task_accepted(task_name):
    active[task_name] += 1
    total[task_name] += 1


def task_ready(task_name):
    active[task_name] -= 1
    if active[task_name] == 0:
        active.pop(task_name, None)
