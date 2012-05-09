# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import with_statement

from itertools import starmap

from celery.utils import uuid

#: global list of functions defining tasks that should be
#: added to all apps.
_shared_tasks = []


def shared_task(constructor):
    """Decorator that specifies that the decorated function is a function
    that generates a built-in task.

    The function will then be called for every new app instance created
    (lazily, so more exactly when the task registry for that app is needed).
    """
    _shared_tasks.append(constructor)
    return constructor


def load_shared_tasks(app):
    """Loads the built-in tasks for an app instance."""
    for constructor in _shared_tasks:
        constructor(app)


@shared_task
def add_backend_cleanup_task(app):
    """The backend cleanup task can be used to clean up the default result
    backend.

    This task is also added do the periodic task schedule so that it is
    run every day at midnight, but :program:`celerybeat` must be running
    for this to be effective.

    Note that not all backends do anything for this, what needs to be
    done at cleanup is up to each backend, and some backends
    may even clean up in realtime so that a periodic cleanup is not necessary.

    """

    @app.task(name="celery.backend_cleanup")
    def backend_cleanup():
        app.backend.cleanup()
    return backend_cleanup


@shared_task
def add_unlock_chord_task(app):
    """The unlock chord task is used by result backends that doesn't
    have native chord support.

    It creates a task chain polling the header for completion.

    """
    from celery.canvas import subtask
    from celery import result as _res

    @app.task(name="celery.chord_unlock", max_retries=None)
    def unlock_chord(setid, callback, interval=1, propagate=False,
            max_retries=None, result=None):
        result = _res.TaskSetResult(setid, map(_res.AsyncResult, result))
        j = result.join_native if result.supports_native_join else result.join
        if result.ready():
            subtask(callback).delay(j(propagate=propagate))
        else:
            unlock_chord.retry(countdown=interval, max_retries=max_retries)
    return unlock_chord


@shared_task
def add_map_task(app):
    from celery.canvas import subtask

    @app.task(name="celery.map")
    def xmap(task, it):
        task = subtask(task).type
        return list(map(task, it))


@shared_task
def add_starmap_task(app):
    from celery.canvas import subtask

    @app.task(name="celery.starmap")
    def xstarmap(task, it):
        task = subtask(task).type
        return list(starmap(task, it))


@shared_task
def add_chunk_task(app):
    from celery.canvas import chunks as _chunks

    @app.task(name="celery.chunks")
    def chunks(task, it, n):
        return _chunks.apply_chunks(task, it, n)


@shared_task
def add_group_task(app):
    _app = app
    from celery.canvas import subtask
    from celery.app.state import get_current_task
    from celery.result import from_serializable

    class Group(app.Task):
        app = _app
        name = "celery.group"
        accept_magic_kwargs = False

        def run(self, tasks, result, setid):
            app = self.app
            result = from_serializable(result)
            if self.request.is_eager or app.conf.CELERY_ALWAYS_EAGER:
                return app.TaskSetResult(result.id,
                        [subtask(task).apply(taskset_id=setid)
                            for task in tasks])
            with app.default_producer() as pub:
                [subtask(task).apply_async(taskset_id=setid, publisher=pub)
                        for task in tasks]
            parent = get_current_task()
            if parent:
                parent.request.children.append(result)
            return result

        def prepare(self, options, tasks, **kwargs):
            r = []
            options["taskset_id"] = group_id = \
                    options.setdefault("task_id", uuid())
            for task in tasks:
                opts = task.options
                opts["taskset_id"] = group_id
                try:
                    tid = opts["task_id"]
                except KeyError:
                    tid = opts["task_id"] = uuid()
                r.append(self.AsyncResult(tid))
            return tasks, self.app.TaskSetResult(group_id, r), group_id

        def apply_async(self, args=(), kwargs={}, **options):
            if self.app.conf.CELERY_ALWAYS_EAGER:
                return self.apply(args, kwargs, **options)
            tasks, result, gid = self.prepare(options, **kwargs)
            super(Group, self).apply_async(
                    (list(tasks), result, gid), **options)
            return result

        def apply(self, args=(), kwargs={}, **options):
            tasks, result, gid = self.prepare(options, **kwargs)
            return super(Group, self).apply((tasks, result, gid), **options)
    return Group


@shared_task
def add_chain_task(app):
    from celery.canvas import maybe_subtask
    _app = app

    class Chain(app.Task):
        app = _app
        name = "celery.chain"
        accept_magic_kwargs = False

        def apply_async(self, args=(), kwargs={}, **options):
            if self.app.conf.CELERY_ALWAYS_EAGER:
                return self.apply(args, kwargs, **options)
            tasks = [maybe_subtask(task).clone(task_id=uuid(), **kwargs)
                        for task in kwargs["tasks"]]
            reduce(lambda a, b: a.link(b), tasks)
            tasks[0].apply_async()
            results = [task.type.AsyncResult(task.options["task_id"])
                            for task in tasks]
            reduce(lambda a, b: a.set_parent(b), reversed(results))
            return results[-1]

        def apply(self, args=(), kwargs={}, **options):
            tasks = [maybe_subtask(task).clone() for task in kwargs["tasks"]]
            res = prev = None
            for task in tasks:
                res = task.apply((prev.get(), ) if prev else ())
                res.parent, prev = prev, res
            return res

    return Chain


@shared_task
def add_chord_task(app):
    """Every chord is executed in a dedicated task, so that the chord
    can be used as a subtask, and this generates the task
    responsible for that."""
    from celery import group
    from celery.canvas import maybe_subtask
    _app = app

    class Chord(app.Task):
        app = _app
        name = "celery.chord"
        accept_magic_kwargs = False
        ignore_result = True

        def run(self, header, body, interval=1, max_retries=None,
                propagate=False, eager=False, **kwargs):
            if not isinstance(header, group):
                header = group(header)
            r = []
            setid = uuid()
            for task in header.tasks:
                opts = task.options
                try:
                    tid = opts["task_id"]
                except KeyError:
                    tid = opts["task_id"] = uuid()
                opts["chord"] = body
                opts["taskset_id"] = setid
                r.append(app.AsyncResult(tid))
            app.backend.on_chord_apply(setid, body,
                                       interval=interval,
                                       max_retries=max_retries,
                                       propagate=propagate,
                                       result=r)
            return header(task_id=setid)

        def apply_async(self, args=(), kwargs={}, task_id=None, **options):
            if self.app.conf.CELERY_ALWAYS_EAGER:
                return self.apply(args, kwargs, **options)
            header, body = (list(kwargs["header"]),
                            maybe_subtask(kwargs["body"]))

            callback_id = body.options.setdefault("task_id", task_id or uuid())
            parent = super(Chord, self).apply_async((header, body), **options)
            body_result = self.AsyncResult(callback_id)
            body_result.parent = parent
            return body_result

        def apply(self, args=(), kwargs={}, **options):
            body = kwargs["body"]
            res = super(Chord, self).apply(args, kwargs, **options)
            return maybe_subtask(body).apply(args=(res.get().join(), ))

    return Chord
