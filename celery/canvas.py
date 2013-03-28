# -*- coding: utf-8 -*-
"""
    celery.canvas
    ~~~~~~~~~~~~~

    Composing task workflows.

    Documentation for these functions are in :mod:`celery`.
    You should not import from this module directly.

"""
from __future__ import absolute_import

from copy import deepcopy
from functools import partial as _partial
from operator import itemgetter
from itertools import chain as _chain

from kombu.utils import cached_property, fxrange, kwdict, reprcall, uuid

from celery._state import current_app
from celery.utils.compat import chain_from_iterable
from celery.result import AsyncResult, GroupResult
from celery.utils.functional import (
    maybe_list, is_list, regen,
    chunks as _chunks,
)
from celery.utils.text import truncate


class _getitem_property(object):
    """Attribute -> dict key descriptor.

    The target object must support ``__getitem__``,
    and optionally ``__setitem__``.

    Example:

        class Me(dict):
            deep = defaultdict(dict)

            foo = _getitem_property('foo')
            deep_thing = _getitem_property('deep.thing')


        >>> me = Me()
        >>> me.foo
        None

        >>> me.foo = 10
        >>> me.foo
        10
        >>> me['foo']
        10

        >>> me.deep_thing = 42
        >>> me.deep_thinge
        42
        >>> me.deep:
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


class Signature(dict):
    """Class that wraps the arguments and execution options
    for a single task invocation.

    Used as the parts in a :class:`group` or to safely
    pass tasks around as callbacks.

    :param task: Either a task class/instance, or the name of a task.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to :meth:`Task.apply_async`.

    Note that if the first argument is a :class:`dict`, the other
    arguments will be ignored and the values in the dict will be used
    instead.

        >>> s = subtask('tasks.add', args=(2, 2))
        >>> subtask(s)
        {'task': 'tasks.add', args=(2, 2), kwargs={}, options={}}

    """
    TYPES = {}
    _type = None

    @classmethod
    def register_type(cls, subclass, name=None):
        cls.TYPES[name or subclass.__name__] = subclass
        return subclass

    @classmethod
    def from_dict(self, d):
        typ = d.get('subtask_type')
        if typ:
            return self.TYPES[typ].from_dict(kwdict(d))
        return Signature(d)

    def __init__(self, task=None, args=None, kwargs=None, options=None,
                 type=None, subtask_type=None, immutable=False, **ex):
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
             immutable=immutable)

    def __call__(self, *partial_args, **partial_kwargs):
        return self.apply_async(partial_args, partial_kwargs)
    delay = __call__

    def apply(self, args=(), kwargs={}, **options):
        """Apply this task locally."""
        # For callbacks: extra args are prepended to the stored args.
        args, kwargs, options = self._merge(args, kwargs, options)
        return self.type.apply(args, kwargs, **options)

    def _merge(self, args=(), kwargs={}, options={}):
        if self.immutable:
            return self.args, self.kwargs, dict(self.options, **options)
        return (tuple(args) + tuple(self.args) if args else self.args,
                dict(self.kwargs, **kwargs) if kwargs else self.kwargs,
                dict(self.options, **options) if options else self.options)

    def clone(self, args=(), kwargs={}, **opts):
        # need to deepcopy options so origins links etc. is not modified.
        args, kwargs, opts = self._merge(args, kwargs, opts)
        s = Signature.from_dict({'task': self.task, 'args': tuple(args),
                                 'kwargs': kwargs, 'options': deepcopy(opts),
                                 'subtask_type': self.subtask_type,
                                 'immutable': self.immutable})
        s._type = self._type
        return s
    partial = clone

    def _freeze(self, _id=None):
        opts = self.options
        try:
            tid = opts['task_id']
        except KeyError:
            tid = opts['task_id'] = _id or uuid()
        return self.AsyncResult(tid)

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
            self.immutable = immutable
        self.options.update(options)
        return self

    def apply_async(self, args=(), kwargs={}, **options):
        # For callbacks: extra args are prepended to the stored args.
        args, kwargs, options = self._merge(args, kwargs, options)
        return self._apply_async(args, kwargs, **options)

    def append_to_list_option(self, key, value):
        items = self.options.setdefault(key, [])
        if value not in items:
            items.append(value)
        return value

    def link(self, callback):
        return self.append_to_list_option('link', callback)

    def link_error(self, errback):
        return self.append_to_list_option('link_error', errback)

    def flatten_links(self):
        return list(chain_from_iterable(_chain(
            [[self]],
            (link.flatten_links()
                for link in maybe_list(self.options.get('link')) or [])
        )))

    def __or__(self, other):
        if not isinstance(self, chain) and isinstance(other, chain):
            return chain((self,) + other.tasks)
        elif isinstance(other, chain):
            return chain(*self.tasks + other.tasks)
        elif isinstance(other, Signature):
            if isinstance(self, chain):
                return chain(*self.tasks + (other, ))
            return chain(self, other)
        return NotImplemented

    def __invert__(self):
        return self.apply_async().get()

    def __reduce__(self):
        # for serialization, the task type is lazily loaded,
        # and not stored in the dict itself.
        return subtask, (dict(self), )

    def reprcall(self, *args, **kwargs):
        args, kwargs, _ = self._merge(args, kwargs, {})
        return reprcall(self['task'], args, kwargs)

    def __repr__(self):
        return self.reprcall()

    @cached_property
    def type(self):
        return self._type or current_app.tasks[self['task']]

    @cached_property
    def AsyncResult(self):
        try:
            return self.type.AsyncResult
        except KeyError:  # task not registered
            return AsyncResult

    @cached_property
    def _apply_async(self):
        try:
            return self.type.apply_async
        except KeyError:
            return _partial(current_app.send_task, self['task'])
    id = _getitem_property('options.task_id')
    task = _getitem_property('task')
    args = _getitem_property('args')
    kwargs = _getitem_property('kwargs')
    options = _getitem_property('options')
    subtask_type = _getitem_property('subtask_type')
    immutable = _getitem_property('immutable')


class chain(Signature):

    def __init__(self, *tasks, **options):
        tasks = tasks[0] if len(tasks) == 1 and is_list(tasks[0]) else tasks
        Signature.__init__(
            self, 'celery.chain', (), {'tasks': tasks}, **options
        )
        self.tasks = tasks
        self.subtask_type = 'chain'

    def __call__(self, *args, **kwargs):
        if self.tasks:
            return self.apply_async(args, kwargs)

    @classmethod
    def from_dict(self, d):
        tasks = d['kwargs']['tasks']
        if d['args'] and tasks:
            # partial args passed on to first task in chain (Issue #1057).
            tasks[0]['args'] = d['args'] + tasks[0]['args']
        return chain(*d['kwargs']['tasks'], **kwdict(d['options']))

    @property
    def type(self):
        return self._type or self.tasks[0].type.app.tasks['celery.chain']

    def __repr__(self):
        return ' | '.join(repr(t) for t in self.tasks)
Signature.register_type(chain)


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
            (), {'task': task, 'it': list(it)}, **opts
        )

    @classmethod
    def from_dict(self, d):
        return chunks(*self._unpack_args(d['kwargs']), **d['options'])


class xmap(_basemap):
    _task_name = 'celery.map'

    def __repr__(self):
        task, it = self._unpack_args(self.kwargs)
        return '[%s(x) for x in %s]' % (task.task, truncate(repr(it), 100))
Signature.register_type(xmap)


class xstarmap(_basemap):
    _task_name = 'celery.starmap'

    def __repr__(self):
        task, it = self._unpack_args(self.kwargs)
        return '[%s(*x) for x in %s]' % (task.task, truncate(repr(it), 100))
Signature.register_type(xstarmap)


class chunks(Signature):
    _unpack_args = itemgetter('task', 'it', 'n')

    def __init__(self, task, it, n, **options):
        Signature.__init__(
            self, 'celery.chunks', (),
            {'task': task, 'it': regen(it), 'n': n},
            immutable=True, **options
        )

    @classmethod
    def from_dict(self, d):
        return chunks(*self._unpack_args(d['kwargs']), **d['options'])

    def apply_async(self, args=(), kwargs={}, **opts):
        return self.group().apply_async(args, kwargs, **opts)

    def __call__(self, **options):
        return self.group()(**options)

    def group(self):
        # need to evaluate generators
        task, it, n = self._unpack_args(self.kwargs)
        return group(xstarmap(task, part) for part in _chunks(iter(it), n))

    @classmethod
    def apply_chunks(cls, task, it, n):
        return cls(task, it, n)()
Signature.register_type(chunks)


def _maybe_group(tasks):
    if isinstance(tasks, group):
        tasks = list(tasks.tasks)
    elif isinstance(tasks, Signature):
        tasks = [tasks]
    else:
        tasks = regen(tasks)
    return tasks


class group(Signature):

    def __init__(self, *tasks, **options):
        if len(tasks) == 1:
            tasks = _maybe_group(tasks[0])
        Signature.__init__(
            self, 'celery.group', (), {'tasks': tasks}, **options
        )
        self.tasks, self.subtask_type = tasks, 'group'

    @classmethod
    def from_dict(self, d):
        tasks = d['kwargs']['tasks']
        if d['args'] and tasks:
            # partial args passed on to all tasks in the group (Issue #1057).
            for task in tasks:
                task['args'] = d['args'] + task['args']
        return group(tasks, **kwdict(d['options']))

    def __call__(self, *partial_args, **options):
        tasks = [task.clone() for task in self.tasks]
        if not tasks:
            return
        # taking the app from the first task in the list,
        # there may be a better solution to this, e.g.
        # consolidate tasks with the same app and apply them in
        # batches.
        type = tasks[0].type.app.tasks[self['task']]
        return type(*type.prepare(options, tasks, partial_args))

    def _freeze(self, _id=None):
        opts = self.options
        try:
            gid = opts['group']
        except KeyError:
            gid = opts['group'] = uuid()
        new_tasks, results = [], []
        for task in self.tasks:
            task = maybe_subtask(task).clone()
            results.append(task._freeze())
            new_tasks.append(task)
        self.tasks = self.kwargs['tasks'] = new_tasks
        return GroupResult(gid, results)

    def skew(self, start=1.0, stop=None, step=1.0):
        _next_skew = fxrange(start, stop, step, repeatlast=True).next
        for task in self.tasks:
            task.set(countdown=_next_skew())
        return self

    def __iter__(self):
        return iter(self.tasks)

    def __repr__(self):
        return repr(self.tasks)
Signature.register_type(group)


class chord(Signature):

    def __init__(self, header, body=None, task='celery.chord',
                 args=(), kwargs={}, **options):
        Signature.__init__(
            self, task, args,
            dict(kwargs, header=_maybe_group(header),
                 body=maybe_subtask(body)), **options
        )
        self.subtask_type = 'chord'

    @classmethod
    def from_dict(self, d):
        args, d['kwargs'] = self._unpack_args(**kwdict(d['kwargs']))
        return self(*args, **kwdict(d))

    @staticmethod
    def _unpack_args(header=None, body=None, **kwargs):
        # Python signatures are better at extracting keys from dicts
        # than manually popping things off.
        return (header, body), kwargs

    @property
    def type(self):
        return self._type or self.tasks[0].type.app.tasks['celery.chord']

    def __call__(self, body=None, **kwargs):
        _chord = self.type
        body = (body or self.kwargs['body']).clone()
        kwargs = dict(self.kwargs, body=body, **kwargs)
        if _chord.app.conf.CELERY_ALWAYS_EAGER:
            return self.apply((), kwargs)
        callback_id = body.options.setdefault('task_id', uuid())
        return _chord.AsyncResult(callback_id, parent=_chord(**kwargs))

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

    def __repr__(self):
        if self.body:
            return self.body.reprcall(self.tasks)
        return '<chord without body: %r>' % (self.tasks, )

    tasks = _getitem_property('kwargs.header')
    body = _getitem_property('kwargs.body')
Signature.register_type(chord)


def subtask(varies, *args, **kwargs):
    if not (args or kwargs) and isinstance(varies, dict):
        if isinstance(varies, Signature):
            return varies.clone()
        return Signature.from_dict(varies)
    return Signature(varies, *args, **kwargs)


def maybe_subtask(d):
    if d is not None and isinstance(d, dict) and not isinstance(d, Signature):
        return subtask(d)
    return d
