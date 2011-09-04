from __future__ import absolute_import

from .. import current_app
from ..result import TaskSetResult
from ..utils import gen_unique_id

from .sets import TaskSet, subtask


@current_app.task(name="celery.chord_unlock", max_retries=None)
def _unlock_chord(setid, callback, interval=1, propagate=False,
        max_retries=None):
    result = TaskSetResult.restore(setid)
    if result.ready():
        subtask(callback).delay(result.join(propagate=propagate))
        result.delete()
    else:
        _unlock_chord.retry(countdown=interval, max_retries=max_retries)


class Chord(current_app.Task):
    accept_magic_kwargs = False
    name = "celery.chord"

    def run(self, set, body, interval=1, max_retries=None,
            propagate=False, **kwargs):
        if not isinstance(set, TaskSet):
            set = TaskSet(set)
        r = []
        setid = gen_unique_id()
        for task in set.tasks:
            uuid = gen_unique_id()
            task.options.update(task_id=uuid, chord=body)
            r.append(current_app.AsyncResult(uuid))
        current_app.TaskSetResult(setid, r).save()
        self.backend.on_chord_apply(setid, body, interval,
                                    max_retries=max_retries,
                                    propagate=propagate)
        return set.apply_async(taskset_id=setid)


class chord(object):
    Chord = Chord

    def __init__(self, tasks, **options):
        self.tasks = tasks
        self.options = options

    def __call__(self, body, **options):
        uuid = body.options.setdefault("task_id", gen_unique_id())
        self.Chord.apply_async((list(self.tasks), body), self.options,
                                **options)
        return body.type.app.AsyncResult(uuid)
