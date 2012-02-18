# Example::
#    >>> R = A.apply_async()
#    >>> list(joinall(R))
#    [['A 0', 'A 1', 'A 2', 'A 3', 'A 4', 'A 5', 'A 6', 'A 7', 'A 8', 'A 9'],
#    ['B 0', 'B 1', 'B 2', 'B 3', 'B 4', 'B 5', 'B 6', 'B 7', 'B 8', 'B 9'],
#    ['C 0', 'C 1', 'C 2', 'C 3', 'C 4', 'C 5', 'C 6', 'C 7', 'C 8', 'C 9'],
#    ['D 0', 'D 1', 'D 2', 'D 3', 'D 4', 'D 5', 'D 6', 'D 7', 'D 8', 'D 9'],
#    ['E 0', 'E 1', 'E 2', 'E 3', 'E 4', 'E 5', 'E 6', 'E 7', 'E 8', 'E 9'],
#    ['F 0', 'F 1', 'F 2', 'F 3', 'F 4', 'F 5', 'F 6', 'F 7', 'F 8', 'F 9'],
#    ['G 0', 'G 1', 'G 2', 'G 3', 'G 4', 'G 5', 'G 6', 'G 7', 'G 8', 'G 9'],
#    ['H 0', 'H 1', 'H 2', 'H 3', 'H 4', 'H 5', 'H 6', 'H 7', 'H 8', 'H 9']]
#
#
# Joining the graph asynchronously with a callback
# (Note: only two levels, the deps are considered final
#        when the second task is ready.)
#
#    >>> unlock_graph.apply_async((A.apply_async(),
#    ...                           A_callback.subtask()), countdown=1)


from celery.task import chord, subtask, task, TaskSet
from celery.result import AsyncResult, ResultSet
from collections import deque

@task
def add(x, y):
    return x + y


@task
def make_request(id, url):
    print("GET %r" % (url, ))
    return url


@task
def B_callback(urls, id):
    print("batch %s done" % (id, ))
    return urls


@task
def B(id):
    return chord(make_request.subtask((id, "%s %r" % (id, i, )))
                    for i in xrange(10))(B_callback.subtask((id, )))


@task
def A():
    return TaskSet(B.subtask((c, )) for c in "ABCDEFGH").apply_async()


def joinall(R, timeout=None, propagate=True):
    stack = deque([R])

    try:
        use_native = joinall.backend.supports_native_join
    except AttributeError:
        use_native = False

    while stack:
        res = stack.popleft()
        if isinstance(res, ResultSet):
            j = res.join_native if use_native else res.join
            stack.extend(j(timeout=timeout, propagate=propagate))
        elif isinstance(res, AsyncResult):
            stack.append(res.get(timeout=timeout, propagate=propagate))
        else:
            yield res


@task
def unlock_graph(result, callback, interval=1, propagate=False,
        max_retries=None):
    if result.ready():
        second_level_res = result.get()
        if second_level_res.ready():
            subtask(callback).delay(list(joinall(
                second_level_res, propagate=propagate)))
    else:
        unlock_graph.retry(countdown=interval, max_retries=max_retries)


@task
def A_callback(res):
    print("Everything is done: %r" % (res, ))
    return res


class chord2(object):

    def __init__(self, tasks, **options):
        self.tasks = tasks
        self.options = options

    def __call__(self, body, **options):
        body.options.setdefault("task_id", uuid())
        unlock_graph.apply_async()
