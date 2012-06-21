# -*- coding: utf-8 -*-
"""
    celery.app.builtins
    ~~~~~~~~~~~~~~~~~~~

    Built-in tasks that are always available in all
    app instances. E.g. chord, group and xmap.

"""
from __future__ import absolute_import
from __future__ import with_statement

from itertools import starmap

from celery.state import get_current_worker_task
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

    @app.task(name='celery.backend_cleanup')
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

    @app.task(name='celery.chord_unlock', max_retries=None)
    def unlock_chord(group_id, callback, interval=1, propagate=False,
            max_retries=None, result=None):
        result = _res.GroupResult(group_id, map(_res.AsyncResult, result))
        j = result.join_native if result.supports_native_join else result.join
        if result.ready():
            subtask(callback).delay(j(propagate=propagate))
        else:
            unlock_chord.retry(countdown=interval, max_retries=max_retries)
    return unlock_chord


@shared_task
def add_map_task(app):
    from celery.canvas import subtask

    @app.task(name='celery.map')
    def xmap(task, it):
        task = subtask(task).type
        return list(map(task, it))


@shared_task
def add_starmap_task(app):
    from celery.canvas import subtask

    @app.task(name='celery.starmap')
    def xstarmap(task, it):
        task = subtask(task).type
        return list(starmap(task, it))


@shared_task
def add_chunk_task(app):
    from celery.canvas import chunks as _chunks

    @app.task(name='celery.chunks')
    def chunks(task, it, n):
        return _chunks.apply_chunks(task, it, n)


@shared_task
def add_group_task(app):
    _app = app
    from celery.canvas import subtask
    from celery.result import from_serializable

    class Group(app.Task):
        app = _app
        name = 'celery.group'
        accept_magic_kwargs = False

        def run(self, tasks, result, group_id):
            app = self.app
            result = from_serializable(result)
            if self.request.is_eager or app.conf.CELERY_ALWAYS_EAGER:
                return app.GroupResult(result.id,
                        [subtask(task).apply(group_id=group_id)
                            for task in tasks])
            with app.default_producer() as pub:
                [subtask(task).apply_async(group_id=group_id, publisher=pub,
                                           add_to_parent=False)
                        for task in tasks]
            parent = get_current_worker_task()
            if parent:
                parent.request.children.append(result)
            return result

        def prepare(self, options, tasks, **kwargs):
            r = []
            options['group_id'] = group_id = \
                    options.setdefault('task_id', uuid())
            for task in tasks:
                opts = task.options
                opts['group_id'] = group_id
                try:
                    tid = opts['task_id']
                except KeyError:
                    tid = opts['task_id'] = uuid()
                r.append(self.AsyncResult(tid))
            return tasks, self.app.GroupResult(group_id, r), group_id

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
        name = 'celery.chain'
        accept_magic_kwargs = False

        def apply_async(self, args=(), kwargs={}, **options):
            if self.app.conf.CELERY_ALWAYS_EAGER:
                return self.apply(args, kwargs, **options)
            options.pop('publisher', None)
            group_id = options.pop('group_id', None)
            chord = options.pop('chord', None)
            tasks = [maybe_subtask(t).clone(
                        task_id=options.pop('task_id', uuid()),
                        **options
                    )
                    for t in kwargs['tasks']]
            reduce(lambda a, b: a.link(b), tasks)
            if group_id:
                tasks[-1].set(group_id=group_id)
            if chord:
                tasks[-1].set(chord=chord)
            tasks[0].apply_async()
            results = [task.type.AsyncResult(task.options['task_id'])
                            for task in tasks]
            reduce(lambda a, b: a.set_parent(b), reversed(results))
            return results[-1]

        def apply(self, args=(), kwargs={}, **options):
            tasks = [maybe_subtask(task).clone() for task in kwargs['tasks']]
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
        name = 'celery.chord'
        accept_magic_kwargs = False
        ignore_result = False

        def run(self, header, body, interval=1, max_retries=None,
                propagate=False, eager=False, **kwargs):
            if not isinstance(header, group):
                header = group(header)
            r = []
            group_id = uuid()
            for task in header.tasks:
                opts = task.options
                try:
                    tid = opts['task_id']
                except KeyError:
                    tid = opts['task_id'] = uuid()
                opts['chord'] = body
                opts['group_id'] = group_id
                r.append(app.AsyncResult(tid))
            if eager:
                return header.apply(task_id=group_id)
            app.backend.on_chord_apply(group_id, body,
                                       interval=interval,
                                       max_retries=max_retries,
                                       propagate=propagate,
                                       result=r)
            return header(task_id=group_id)

        def apply_async(self, args=(), kwargs={}, task_id=None, **options):
            if self.app.conf.CELERY_ALWAYS_EAGER:
                return self.apply(args, kwargs, **options)
            group_id = options.pop('group_id', None)
            chord = options.pop('chord', None)
            header, body = (list(kwargs['header']),
                            maybe_subtask(kwargs['body']))
            if group_id:
                body.set(group_id=group_id)
            if chord:
                body.set(chord=chord)
            callback_id = body.options.setdefault('task_id', task_id or uuid())
            parent = super(Chord, self).apply_async((header, body), **options)
            body_result = self.AsyncResult(callback_id)
            body_result.parent = parent
            return body_result

        def apply(self, args=(), kwargs={}, propagate=True, **options):
            body = kwargs['body']
            res = super(Chord, self).apply(args, dict(kwargs, eager=True),
                                           **options)
            return maybe_subtask(body).apply(
                        args=(res.get(propagate=propagate).get().join(), ))

    return Chord
