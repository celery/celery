from kombu.utils import gen_unique_id

from celery import current_app
from celery.result import TaskSetResult
from celery.task.sets import TaskSet, subtask

@current_app.task(name="celery.chord_unlock", max_retries=None)
def _unlock_chord(setid, callback, interval=1, max_retries=None):
    result = TaskSetResult.restore(setid)
    if result.ready():
        return subtask(callback).delay(result.join())
    _unlock_chord.retry(countdown=interval, max_retries=max_retries)


class Chord(current_app.Task):
    name = "celery.chord"

    def run(self, set, body):
        if not isinstance(set, TaskSet):
            set = TaskSet(set)
        r = []
        setid = gen_unique_id()
        for task in set.tasks:
            uuid = gen_unique_id()
            task.options.update(task_id=uuid, chord=body)
            r.append(current_app.AsyncResult(uuid))
        ts = current_app.TaskSetResult(setid, r).save()
        self.backend.on_chord_apply(setid, body)
        return set.apply_async(taskset_id=setid)


class chord(object):
    Chord = Chord

    def __init__(self, tasks):
        self.tasks = tasks

    def __call__(self, body):
        uuid = body.options.setdefault("task_id", gen_unique_id())
        self.Chord.apply_async((list(self.tasks), body))
        return body.type.app.AsyncResult(uuid)
