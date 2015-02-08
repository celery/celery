# -*- coding: utf-8 -*-
"""
    celery.canvas
    ~~~~~~~~~~~~~

    Composing task workflows.

    Documentation for some of these types are in :mod:`celery`.
    You should import these from :mod:`celery` and not this module.


"""
from __future__ import absolute_import

from collections import MutableSequence, deque
from copy import deepcopy
from functools import partial as _partial, reduce
from operator import itemgetter
from itertools import chain as _chain

from kombu.utils import cached_property, fxrange, reprcall, uuid

from celery._state import current_app, get_current_worker_task
from celery.utils.functional import (
    maybe_list, is_list, regen,
    chunks as _chunks,
)
from celery.utils.text import truncate

__all__ = ['Signature', 'chain', 'xmap', 'xstarmap', 'chunks',
           'group', 'chord', 'signature', 'maybe_signature']


class _getitem_property(object):
    """Attribute -> dict key descriptor.

    The target object must support ``__getitem__``,
    and optionally ``__setitem__``.

    Example:

        >>> from collections import defaultdict

        >>> class Me(dict):
        ...     deep = defaultdict(dict)
        ...
        ...     foo = _getitem_property('foo')
        ...     deep_thing = _getitem_property('deep.thing')


        >>> me = Me()
        >>> me.foo
        None

        >>> me.foo = 10
        >>> me.foo
        10
        >>> me['foo']
        10

        >>> me.deep_thing = 42
        >>> me.deep_thing
        42
        >>> me.deep
        defaultdict(<type 'dict'>, {'thing': 42})

    """

    def __init__(self, keypath):
        path, _, self.key = keypath.rpartition('.')
        self.path = path.split('.') if path else None

    def _path(self, obj):
        return (reduce(lambda d, k: d[k], [obj] + self.path) if self.path
                else obj)

    def __get__(self, obj, type=None):
        if obj is None:
            return type
        return self._path(obj).get(self.key)

    def __set__(self, obj, value):
        self._path(obj)[self.key] = value


def maybe_unroll_group(g):
    """Unroll group with only one member."""
    # Issue #1656
    try:
        size = len(g.tasks)
    except TypeError:
        try:
            size = g.tasks.__length_hint__()
        except (AttributeError, TypeError):
            pass
        else:
            return list(g.tasks)[0] if size == 1 else g
    else:
        return g.tasks[0] if size == 1 else g


def task_name_from(task):
    return getattr(task, 'name', task)


def _upgrade(fields, sig):
    """Used by custom signatures in .from_dict, to keep common fields."""
    sig.update(chord_size=fields.get('chord_size'))
    return sig


class Signature(dict):
    """Class that wraps the arguments and execution options
    for a single task invocation.

    Used as the parts in a :class:`group` and other constructs,
    or to pass tasks around as callbacks while being compatible
    with serializers with a strict type subset.

    :param task: Either a task class/instance, or the name of a task.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to :meth:`Task.apply_async`.

    Note that if the first argument is a :class:`dict`, the other
    arguments will be ignored and the values in the dict will be used
    instead.

        >>> s = signature('tasks.add', args=(2, 2))
        >>> signature(s)
        {'task': 'tasks.add', args=(2, 2), kwargs={}, options={}}

    """
    TYPES = {}
    _app = _type = None

    @classmethod
    def register_type(cls, subclass, name=None):
        cls.TYPES[name or subclass.__name__] = subclass
        return subclass

    @classmethod
    def from_dict(self, d, app=None):
        typ = d.get('subtask_type')
        if typ:
            return self.TYPES[typ].from_dict(d, app=app)
        return Signature(d, app=app)

    def __init__(self, task=None, args=None, kwargs=None, options=None,
                 type=None, subtask_type=None, immutable=False,
                 app=None, **ex):
        self._app = app
        init = dict.__init__

        if isinstance(task, dict):
            return init(self, task)  # works like dict(d)

        # Also supports using task class/instance instead of string name.
        try:
            task_name = task.name
        except AttributeError:
            task_name = task
        else:
            self._type = task

        init(self,
             task=task_name, args=tuple(args or ()),
             kwargs=kwargs or {},
             options=dict(options or {}, **ex),
             subtask_type=subtask_type,
             immutable=immutable,
             chord_size=None)

    def __call__(self, *partial_args, **partial_kwargs):
        args, kwargs, _ = self._merge(partial_args, partial_kwargs, None)
        return self.type(*args, **kwargs)

    def delay(self, *partial_args, **partial_kwargs):
        return self.apply_async(partial_args, partial_kwargs)

    def apply(self, args=(), kwargs={}, **options):
        """Apply this task locally."""
        # For callbacks: extra args are prepended to the stored args.
        args, kwargs, options = self._merge(args, kwargs, options)
        return self.type.apply(args, kwargs, **options)

    def _merge(self, args=(), kwargs={}, options={}):
        if self.immutable:
            return (self.args, self.kwargs,
                    dict(self.options, **options) if options else self.options)
        return (tuple(args) + tuple(self.args) if args else self.args,
                dict(self.kwargs, **kwargs) if kwargs else self.kwargs,
                dict(self.options, **options) if options else self.options)

    def clone(self, args=(), kwargs={}, **opts):
        # need to deepcopy options so origins links etc. is not modified.
        if args or kwargs or opts:
            args, kwargs, opts = self._merge(args, kwargs, opts)
        else:
            args, kwargs, opts = self.args, self.kwargs, self.options
        s = Signature.from_dict({'task': self.task, 'args': tuple(args),
                                 'kwargs': kwargs, 'options': deepcopy(opts),
                                 'subtask_type': self.subtask_type,
                                 'chord_size': self.chord_size,
                                 'immutable': self.immutable}, app=self._app)
        s._type = self._type
        return s
    partial = clone

    def freeze(self, _id=None, group_id=None, chord=None, root_id=None):
        opts = self.options
        try:
            tid = opts['task_id']
        except KeyError:
            tid = opts['task_id'] = _id or uuid()
        root_id = opts.setdefault('root_id', root_id)
        if 'reply_to' not in opts:
            opts['reply_to'] = self.app.oid
        if group_id:
            opts['group_id'] = group_id
        if chord:
            opts['chord'] = chord
        return self.AsyncResult(tid)
    _freeze = freeze

    def replace(self, args=None, kwargs=None, options=None):
        s = self.clone()
        if args is not None:
            s.args = args
        if kwargs is not None:
            s.kwargs = kwargs
        if options is not None:
            s.options = options
        return s

    def set(self, immutable=None, **options):
        if immutable is not None:
            self.set_immutable(immutable)
        self.options.update(options)
        return self

    def set_immutable(self, immutable):
        self.immutable = immutable

    def apply_async(self, args=(), kwargs={}, route_name=None, **options):
        try:
            _apply = self._apply_async
        except IndexError:  # no tasks for chain, etc to find type
            return
        # For callbacks: extra args are prepended to the stored args.
        if args or kwargs or options:
            args, kwargs, options = self._merge(args, kwargs, options)
        else:
            args, kwargs, options = self.args, self.kwargs, self.options
        return _apply(args, kwargs, **options)

    def append_to_list_option(self, key, value):
        items = self.options.setdefault(key, [])
        if not isinstance(items, MutableSequence):
            items = self.options[key] = [items]
        if value not in items:
            items.append(value)
        return value

    def link(self, callback):
        return self.append_to_list_option('link', callback)

    def link_error(self, errback):
        return self.append_to_list_option('link_error', errback)

    def flatten_links(self):
        return list(_chain.from_iterable(_chain(
            [[self]],
            (link.flatten_links()
                for link in maybe_list(self.options.get('link')) or [])
        )))

    def __or__(self, other):
        if isinstance(other, group):
            other = maybe_unroll_group(other)
        if not isinstance(self, chain) and isinstance(other, chain):
            return chain((self, ) + other.tasks, app=self._app)
        elif isinstance(other, chain):
            return chain(*self.tasks + other.tasks, app=self._app)
        elif isinstance(other, Signature):
            if isinstance(self, chain):
                return chain(*self.tasks + (other, ), app=self._app)
            return chain(self, other, app=self._app)
        return NotImplemented

    def __deepcopy__(self, memo):
        memo[id(self)] = self
        return dict(self)

    def __invert__(self):
        return self.apply_async().get()

    def __reduce__(self):
        # for serialization, the task type is lazily loaded,
        # and not stored in the dict itself.
        return signature, (dict(self), )

    def __json__(self):
        return dict(self)

    def reprcall(self, *args, **kwargs):
        args, kwargs, _ = self._merge(args, kwargs, {})
        return reprcall(self['task'], args, kwargs)

    def election(self):
        type = self.type
        app = type.app
        tid = self.options.get('task_id') or uuid()

        with app.producer_or_acquire(None) as P:
            props = type.backend.on_task_call(P, tid)
            app.control.election(tid, 'task', self.clone(task_id=tid, **props),
                                 connection=P.connection)
            return type.AsyncResult(tid)

    def __repr__(self):
        return self.reprcall()

    @property
    def name(self):
        # for duck typing compatibility with Task.name
        return self.task

    @cached_property
    def type(self):
        return self._type or self.app.tasks[self['task']]

    @cached_property
    def app(self):
        return self._app or current_app

    @cached_property
    def AsyncResult(self):
        try:
            return self.type.AsyncResult
        except KeyError:  # task not registered
            return self.app.AsyncResult

    @cached_property
    def _apply_async(self):
        try:
            return self.type.apply_async
        except KeyError:
            return _partial(self.app.send_task, self['task'])
    id = _getitem_property('options.task_id')
    task = _getitem_property('task')
    args = _getitem_property('args')
    kwargs = _getitem_property('kwargs')
    options = _getitem_property('options')
    subtask_type = _getitem_property('subtask_type')
    chord_size = _getitem_property('chord_size')
    immutable = _getitem_property('immutable')


@Signature.register_type
class chain(Signature):
    tasks = _getitem_property('kwargs.tasks')

    def __init__(self, *tasks, **options):
        tasks = (regen(tasks[0]) if len(tasks) == 1 and is_list(tasks[0])
                 else tasks)
        Signature.__init__(
            self, 'celery.chain', (), {'tasks': tasks}, **options
        )
        self.subtask_type = 'chain'

    def __call__(self, *args, **kwargs):
        if self.tasks:
            return self.apply_async(args, kwargs)

    def apply_async(self, args=(), kwargs={}, **options):
        # python is best at unpacking kwargs, so .run is here to do that.
        app = self.app
        if app.conf.CELERY_ALWAYS_EAGER:
            return self.apply(args, kwargs, **options)
        return self.run(args, kwargs, app=app, **(
            dict(self.options, **options) if options else self.options))

    def run(self, args=(), kwargs={}, group_id=None, chord=None,
            task_id=None, link=None, link_error=None,
            publisher=None, producer=None, root_id=None, app=None, **options):
        app = app or self.app
        args = (tuple(args) + tuple(self.args)
                if args and not self.immutable else self.args)
        tasks, results = self.prepare_steps(
            args, self.tasks, root_id, link_error, app,
            task_id, group_id, chord,
        )
        if results:
            # make sure we can do a link() and link_error() on a chain object.
            if link:
                tasks[-1].set(link=link)
            tasks[0].apply_async(**options)
            return results[-1]

    def prepare_steps(self, args, tasks,
                      root_id=None, link_error=None, app=None,
                      last_task_id=None, group_id=None, chord_body=None,
                      from_dict=Signature.from_dict):
        app = app or self.app
        steps = deque(tasks)
        next_step = prev_task = prev_res = None
        tasks, results = [], []
        i = 0
        while steps:
            task = steps.popleft()

            if not isinstance(task, Signature):
                task = from_dict(task, app=app)
            if isinstance(task, group):
                task = maybe_unroll_group(task)

            # first task gets partial args from chain
            task = task.clone(args) if not i else task.clone()

            if isinstance(task, chain):
                # splice the chain
                steps.extendleft(reversed(task.tasks))
                continue
            elif isinstance(task, group) and steps:
                # automatically upgrade group(...) | s to chord(group, s)
                try:
                    next_step = steps.popleft()
                    # for chords we freeze by pretending it's a normal
                    # signature instead of a group.
                    res = Signature.freeze(next_step, root_id=root_id)
                    task = chord(
                        task, body=next_step,
                        task_id=res.task_id, root_id=root_id,
                    )
                except IndexError:
                    pass  # no callback, so keep as group.

            if steps:
                res = task.freeze(root_id=root_id)
            else:
                # chain(task_id=id) means task id is set for the last task
                # in the chain.  If the chord is part of a chord/group
                # then that chord/group must synchronize based on the
                # last task in the chain, so we only set the group_id and
                # chord callback for the last task.
                res = task.freeze(
                    last_task_id,
                    root_id=root_id, group_id=group_id, chord=chord_body,
                )
            root_id = res.id if root_id is None else root_id
            i += 1

            if prev_task:
                # link previous task to this task.
                prev_task.link(task)
                # set AsyncResult.parent
                if not res.parent:
                    res.parent = prev_res

            if link_error:
                task.set(link_error=link_error)

            if not isinstance(prev_task, chord):
                results.append(res)
                tasks.append(task)
            prev_task, prev_res = task, res

        return tasks, results

    def apply(self, args=(), kwargs={}, **options):
        last, fargs = None, args
        for task in self.tasks:
            res = task.clone(fargs).apply(
                last and (last.get(), ), **dict(self.options, **options))
            res.parent, last, fargs = last, res, None
        return last

    @classmethod
    def from_dict(self, d, app=None):
        tasks = d['kwargs']['tasks']
        if tasks:
            if isinstance(tasks, tuple):  # aaaargh
                tasks = d['kwargs']['tasks'] = list(tasks)
            # First task must be signature object to get app
            tasks[0] = maybe_signature(tasks[0], app=app)
        return _upgrade(d, chain(*tasks, app=app, **d['options']))

    @property
    def app(self):
        app = self._app
        if app is None:
            try:
                app = self.tasks[0]._app
            except (KeyError, IndexError):
                pass
        return app or current_app

    def __repr__(self):
        return ' | '.join(repr(t) for t in self.tasks)


class _basemap(Signature):
    _task_name = None
    _unpack_args = itemgetter('task', 'it')

    def __init__(self, task, it, **options):
        Signature.__init__(
            self, self._task_name, (),
            {'task': task, 'it': regen(it)}, immutable=True, **options
        )

    def apply_async(self, args=(), kwargs={}, **opts):
        # need to evaluate generators
        task, it = self._unpack_args(self.kwargs)
        return self.type.apply_async(
            (), {'task': task, 'it': list(it)},
            route_name=task_name_from(self.kwargs.get('task')), **opts
        )

    @classmethod
    def from_dict(cls, d, app=None):
        return _upgrade(
            d, cls(*cls._unpack_args(d['kwargs']), app=app, **d['options']),
        )


@Signature.register_type
class xmap(_basemap):
    _task_name = 'celery.map'

    def __repr__(self):
        task, it = self._unpack_args(self.kwargs)
        return '[{0}(x) for x in {1}]'.format(task.task,
                                              truncate(repr(it), 100))


@Signature.register_type
class xstarmap(_basemap):
    _task_name = 'celery.starmap'

    def __repr__(self):
        task, it = self._unpack_args(self.kwargs)
        return '[{0}(*x) for x in {1}]'.format(task.task,
                                               truncate(repr(it), 100))


@Signature.register_type
class chunks(Signature):
    _unpack_args = itemgetter('task', 'it', 'n')

    def __init__(self, task, it, n, **options):
        Signature.__init__(
            self, 'celery.chunks', (),
            {'task': task, 'it': regen(it), 'n': n},
            immutable=True, **options
        )

    @classmethod
    def from_dict(self, d, app=None):
        return _upgrade(
            d, chunks(*self._unpack_args(
                d['kwargs']), app=app, **d['options']),
        )

    def apply_async(self, args=(), kwargs={}, **opts):
        return self.group().apply_async(
            args, kwargs,
            route_name=task_name_from(self.kwargs.get('task')), **opts
        )

    def __call__(self, **options):
        return self.apply_async(**options)

    def group(self):
        # need to evaluate generators
        task, it, n = self._unpack_args(self.kwargs)
        return group((xstarmap(task, part, app=self._app)
                      for part in _chunks(iter(it), n)),
                     app=self._app)

    @classmethod
    def apply_chunks(cls, task, it, n, app=None):
        return cls(task, it, n, app=app)()


def _maybe_group(tasks):
    if isinstance(tasks, group):
        tasks = list(tasks.tasks)
    elif isinstance(tasks, Signature):
        tasks = [tasks]
    else:
        tasks = regen(tasks)
    return tasks


@Signature.register_type
class group(Signature):
    tasks = _getitem_property('kwargs.tasks')

    def __init__(self, *tasks, **options):
        if len(tasks) == 1:
            tasks = _maybe_group(tasks[0])
        Signature.__init__(
            self, 'celery.group', (), {'tasks': tasks}, **options
        )
        self.subtask_type = 'group'

    @classmethod
    def from_dict(self, d, app=None):
        return _upgrade(
            d, group(d['kwargs']['tasks'], app=app, **d['options']),
        )

    def _prepared(self, tasks, partial_args, group_id, root_id, app, dict=dict,
                  Signature=Signature, from_dict=Signature.from_dict):
        for task in tasks:
            if isinstance(task, dict):
                if isinstance(task, Signature):
                    # local sigs are always of type Signature, and we
                    # clone them to make sure we do not modify the originals.
                    task = task.clone()
                else:
                    # serialized sigs must be converted to Signature.
                    task = from_dict(task, app=app)
                if isinstance(task, group):
                    # needs yield_from :(
                    unroll = task._prepared(
                        task.tasks, partial_args, group_id, root_id, app,
                    )
                    for taskN, resN in unroll:
                        yield taskN, resN
                else:
                    if partial_args and not task.immutable:
                        task.args = tuple(partial_args) + tuple(task.args)
                    yield task, task.freeze(group_id=group_id, root_id=root_id)

    def _apply_tasks(self, tasks, producer=None, app=None,
                     add_to_parent=None, **options):
        app = app or self.app
        with app.producer_or_acquire(producer) as producer:
            for sig, res in tasks:
                sig.apply_async(producer=producer, add_to_parent=False,
                                **options)
                yield res

    def _freeze_gid(self, options):
        # remove task_id and use that as the group_id,
        # if we don't remove it then every task will have the same id...
        options = dict(self.options, **options)
        options['group_id'] = group_id = (
            options.pop('task_id', uuid()))
        return options, group_id, options.get('root_id')

    def apply_async(self, args=(), kwargs=None, add_to_parent=True,
                    producer=None, **options):
        app = self.app
        if app.conf.CELERY_ALWAYS_EAGER:
            return self.apply(args, kwargs, **options)
        if not self.tasks:
            return self.freeze()

        options, group_id, root_id = self._freeze_gid(options)
        tasks = self._prepared(self.tasks, args, group_id, root_id, app)
        result = self.app.GroupResult(
            group_id, list(self._apply_tasks(tasks, producer, app, **options)),
        )
        parent_task = get_current_worker_task()
        if add_to_parent and parent_task:
            parent_task.add_trail(result)
        return result

    def apply(self, args=(), kwargs={}, **options):
        app = self.app
        if not self.tasks:
            return self.freeze()  # empty group returns GroupResult
        options, group_id, root_id = self._freeze_gid(options)
        tasks = self._prepared(self.tasks, args, group_id, root_id, app)
        return app.GroupResult(group_id, [
            sig.apply(**options) for sig, _ in tasks
        ])

    def set_immutable(self, immutable):
        for task in self.tasks:
            task.set_immutable(immutable)

    def link(self, sig):
        # Simply link to first task
        sig = sig.clone().set(immutable=True)
        return self.tasks[0].link(sig)

    def link_error(self, sig):
        sig = sig.clone().set(immutable=True)
        return self.tasks[0].link_error(sig)

    def __call__(self, *partial_args, **options):
        return self.apply_async(partial_args, **options)

    def _freeze_unroll(self, new_tasks, group_id, chord, root_id):
        stack = deque(self.tasks)
        while stack:
            task = maybe_signature(stack.popleft(), app=self._app).clone()
            if isinstance(task, group):
                stack.extendleft(task.tasks)
            else:
                new_tasks.append(task)
                yield task.freeze(group_id=group_id,
                                  chord=chord, root_id=root_id)

    def freeze(self, _id=None, group_id=None, chord=None, root_id=None):
        opts = self.options
        try:
            gid = opts['task_id']
        except KeyError:
            gid = opts['task_id'] = uuid()
        if group_id:
            opts['group_id'] = group_id
        if chord:
            opts['chord'] = chord
        root_id = opts.setdefault('root_id', root_id)
        new_tasks = []
        # Need to unroll subgroups early so that chord gets the
        # right result instance for chord_unlock etc.
        results = list(self._freeze_unroll(
            new_tasks, group_id, chord, root_id,
        ))
        if isinstance(self.tasks, MutableSequence):
            self.tasks[:] = new_tasks
        else:
            self.tasks = new_tasks
        return self.app.GroupResult(gid, results)
    _freeze = freeze

    def skew(self, start=1.0, stop=None, step=1.0):
        it = fxrange(start, stop, step, repeatlast=True)
        for task in self.tasks:
            task.set(countdown=next(it))
        return self

    def __iter__(self):
        return iter(self.tasks)

    def __repr__(self):
        return repr(self.tasks)

    @property
    def app(self):
        app = self._app
        if app is None:
            try:
                app = self.tasks[0].app
            except (KeyError, IndexError):
                pass
        return app if app is not None else current_app


@Signature.register_type
class chord(Signature):

    def __init__(self, header, body=None, task='celery.chord',
                 args=(), kwargs={}, **options):
        Signature.__init__(
            self, task, args,
            dict(kwargs, header=_maybe_group(header),
                 body=maybe_signature(body, app=self._app)), **options
        )
        self.subtask_type = 'chord'

    def freeze(self, *args, **kwargs):
        return self.body.freeze(*args, **kwargs)

    @classmethod
    def from_dict(self, d, app=None):
        args, d['kwargs'] = self._unpack_args(**d['kwargs'])
        return _upgrade(d, self(*args, app=app, **d))

    @staticmethod
    def _unpack_args(header=None, body=None, **kwargs):
        # Python signatures are better at extracting keys from dicts
        # than manually popping things off.
        return (header, body), kwargs

    @cached_property
    def app(self):
        return self._get_app(self.body)

    def _get_app(self, body=None):
        app = self._app
        if app is None:
            app = self.tasks[0]._app
            if app is None and body is not None:
                app = body._app
        return app if app is not None else current_app

    def apply_async(self, args=(), kwargs={}, task_id=None,
                    producer=None, publisher=None, connection=None,
                    router=None, result_cls=None, **options):
        args = (tuple(args) + tuple(self.args)
                if args and not self.immutable else self.args)
        body = kwargs.get('body') or self.kwargs['body']
        kwargs = dict(self.kwargs, **kwargs)
        body = body.clone(**options)
        app = self._get_app(body)
        tasks = (self.tasks.clone() if isinstance(self.tasks, group)
                 else group(self.tasks))
        if app.conf.CELERY_ALWAYS_EAGER:
            return self.apply((), kwargs,
                              body=body, task_id=task_id, **options)
        return self.run(tasks, body, args, task_id=task_id, **options)

    def apply(self, args=(), kwargs={}, propagate=True, body=None, **options):
        body = self.body if body is None else body
        tasks = (self.tasks.clone() if isinstance(self.tasks, group)
                 else group(self.tasks))
        return body.apply(
            args=(tasks.apply().get(propagate=propagate), ),
        )

    def _traverse_tasks(self, tasks, value=None):
        stack = deque(tasks)
        while stack:
            task = stack.popleft()
            if isinstance(task, group):
                stack.extend(task.tasks)
            else:
                yield task if value is None else value

    def __length_hint__(self):
        return sum(self._traverse_tasks(self.tasks, 1))

    def run(self, header, body, partial_args, app=None, interval=None,
            countdown=1, max_retries=None, propagate=None, eager=False,
            task_id=None, **options):
        app = app or self._get_app(body)
        propagate = (app.conf.CELERY_CHORD_PROPAGATES
                     if propagate is None else propagate)
        group_id = uuid()
        root_id = body.options.get('root_id')
        body.chord_size = self.__length_hint__()
        options = dict(self.options, **options) if options else self.options
        if options:
            body.options.update(options)

        results = header.freeze(
            group_id=group_id, chord=body, root_id=root_id).results
        bodyres = body.freeze(task_id, root_id=root_id)

        parent = app.backend.apply_chord(
            header, partial_args, group_id, body,
            interval=interval, countdown=countdown,
            options=options, max_retries=max_retries,
            propagate=propagate, result=results)
        bodyres.parent = parent
        return bodyres

    def __call__(self, body=None, **options):
        return self.apply_async((), {'body': body} if body else {}, **options)

    def clone(self, *args, **kwargs):
        s = Signature.clone(self, *args, **kwargs)
        # need to make copy of body
        try:
            s.kwargs['body'] = s.kwargs['body'].clone()
        except (AttributeError, KeyError):
            pass
        return s

    def link(self, callback):
        self.body.link(callback)
        return callback

    def link_error(self, errback):
        self.body.link_error(errback)
        return errback

    def set_immutable(self, immutable):
        # changes mutability of header only, not callback.
        for task in self.tasks:
            task.set_immutable(immutable)

    def __repr__(self):
        if self.body:
            return self.body.reprcall(self.tasks)
        return '<chord without body: {0.tasks!r}>'.format(self)

    tasks = _getitem_property('kwargs.header')
    body = _getitem_property('kwargs.body')


def signature(varies, *args, **kwargs):
    if isinstance(varies, dict):
        if isinstance(varies, Signature):
            return varies.clone()
        return Signature.from_dict(varies)
    return Signature(varies, *args, **kwargs)
subtask = signature   # XXX compat


def maybe_signature(d, app=None):
    if d is not None:
        if isinstance(d, dict):
            if not isinstance(d, Signature):
                d = signature(d)
        elif isinstance(d, list):
            return [maybe_signature(s, app=app) for s in d]

        if app is not None:
            d._app = app
        return d

maybe_subtask = maybe_signature  # XXX compat
