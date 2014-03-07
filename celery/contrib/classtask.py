# -*- coding: utf-8 -*-
"""
celery.contrib.classtask
========================

Metaclass that supports creating tasks out of classes.

Use of this metaclass turns a class and its subclasses into a celery task. With
minimal differences, the task behaves similarly to a task created by the
`app.task` function decorator.


Key Concepts
------------

* This metaclass makes instance initialization "lazy".  That is, the
  conventional python object instantiation process is intercepted, and
  :meth:`__init__` is explicitly _not_ called.  This delayed _initialization_ of
  the instance object prevents unnecessary work being done in the synchronous
  process.
* References to the `args` and `kwargs` given to __init__ are held by the
  instance object. They are passed as parameters in the `task` message to fully
  initialize the class in the external process.
* After class instantiation, :meth:`enqueue` may be called on the instance
  object analogously to :meth:`delay` or :meth:`apply_async`. :meth:`enqueue`
  takes no `args` or `kwargs`. It does however accept the same `options` as
  :meth:`apply_async`.


Use
---

1) Add `__metaclass__ = ClassTask` to the class definition. Among other
   things, your class is now a subclass of :class:`ClassTaskBase`.
2) Override the :meth:`run` method.  This code, along with whatever code
   is in :meth:`__init__`, will be executed by the external process.
3) To use your class, instantiate it as normal--i.e.
   `my_instance = MyClass(*args, **kwargs)`. Then call :meth:`enqueue` on the
   instantiated object--i.e. `my_instance.enqueue()`. The code in
   :meth:`__init__` is *not* ran in-process, but rather delayed for the external
   process to run.
4) If for some reason you need MyClass initialized in-process, call :meth:`init`
   on the instance object--i.e. `my_instance.init()`.  In this case, the
   :meth:`__init__` code will run both in-process *and* in the external process.
5) One method decorator is provided as auxiliary to this metaclass.
   If a method is decorated with :decorator:`auto_init`, that method can
   be accessed without first calling :meth:`init`, and :meth:`init` will
   _automatically_ be called if :meth:`__init__` has not been ran on the
   instance object already.
6) Access to any :decorator:`staticmethod` or :decorator:`classmethod` is
   allowed even if :meth:`__init__` hasn't been called on the instance.


Metaclass Strategy
------------------

* The metaclass fundamentally delays execution of :meth"`__init__`, and it
  stores the `args` and `kwargs` passed to the class callable as references
  :attr:`_args` and :attr:`_kwargs` on the new instance object.
* The metaclass ensures that the class is a subclass of :class:`ClassTaskBase`
  which in turn is a subclass of :class:`celery.Task`.  :class:`ClassTaskBase`
  adds the additional methods (like enqueue and run_async) necessary to turn a
  generic class into a celery task.
* Any attributes that might be "dangerous" to call before :meth:`__init__` is
  called are wrapped to throw a runtime error if :meth:`__init__` is not called
  before the attribute.  The "dangerous" attributes are any public bound
  methods (those that do not begin with an underscore), and any
  :decorator:`@properties`.

"""
from __future__ import absolute_import

from functools import wraps
import itertools

from celery.app.task import TaskType
from celery.local import Proxy

from celery import current_app, Task

__all__ = ['ClassTask', 'auto_init']


class ClassTask(TaskType):
    """A metaclass to enable asynchronous execution of any class.

    Examples:
        >>> class Math(object):
        ...     __metaclass__ = ClassTask
        ...     def __init__(self, a, b):
        ...         self.a = a
        ...         self.b = b
        ...     def run(self):
        ...         return self.mult() * 2
        ...     def mult(self):
        ...         return self.a * self.b
        >>> b = Math(4, 2)
        >>> b.mult()  # doctest:+ELLIPSIS
        Traceback (most recent call last):
        ...
        AttributeError: The instance object <@task: async.Math of ttam:...> has not yet been initialized...
        >>> b.init().mult()
        8
        >>> b.enqueue().get()
        16

    """

    class ClassTaskBase(Task):

        def init(self):
            """Public method to explicitly call :meth:`__init__` on the instance
            object.

            :throws AttributeError: When the instance has already been
                                    initialized.
            """
            if not self._initialized:
                self.__init__(*self._args, **self._kwargs)
                self._initialized = True
            else:
                # only initialize once. this is a safety precaution. if this
                # error is thrown, you're doing something wrong.  perhaps at
                # some point, this should be changed to simply logging an error
                # rather than throwing a runtime exception
                raise AttributeError("{} has already been initialized.\n"
                                     "  args: {}\n"
                                     "  kwargs: {}".format(self.__class__.__name__,
                                                           self._args,
                                                           self._kwargs))
            return self  # return self to allow call chaining

        def enqueue(self, **options):
            """Used to execute the instance object. Analagous to :meth:`delay`
            and :meth:`apply_async`, but takes only the optional control
            arguments given to :meth:`apply_async`.  Any `args` and `kwargs`
            needed for execution should have been passed on object
            instantiation.
            """
            return self.apply_async(self._args, self._kwargs, **options)

        def run(self):
            """The `ClassTask` :meth:`run` method is not allowed to take
            arguments. Rather, all class state must be established in
            :meth:`__init__`.
            """
            raise NotImplementedError("ClassTasks must define the run method.")

        def __call__(self, *args, **kwargs):
            # override to instantiate class object, then init and run
            return self.__class__(*args, **kwargs).init().run()

        def delay(self):
            # ClassTask delay is not allowed to take arguments
            return super(ClassTask.ClassTaskBase, self).delay()

        def apply_async(self, args=None, kwargs=None, task_id=None,
                        producer=None, link=None, link_error=None, **options):
            # override to pull _args and _kwargs off of the instance object
            _apl_as = super(ClassTask.ClassTaskBase, self).apply_async
            return _apl_as(self._args, self._kwargs, task_id, producer, link,
                           link_error, **options)

        def apply(self, args=None, kwargs=None, link=None, link_error=None,
                  **options):
            # override to pull _args and _kwargs off of the instance object
            _apl = super(ClassTask.ClassTaskBase, self).apply
            return _apl(self._args, self._kwargs, link, link_error, **options)

        def retry(self, args=None, kwargs=None, exc=None, throw=True, eta=None,
                  countdown=None, max_retries=None, **options):
            # override to pull _args and _kwargs off of the instance object
            _retry = super(ClassTask.ClassTaskBase, self).retry
            return _retry(self._args, self._kwargs, exc, throw, eta, countdown,
                          max_retries, **options)

        @classmethod
        def get_task(cls):
            return cls._app.tasks[cls.name]


    @staticmethod
    def _ensure_init(attr):
        # wrapper (i.e. decorator) for methods and properties to make sure
        #   __init__ gets called before use
        # decided throwing an explicit runtime error is safer than guessing
        #  whether a method is designed to be public or private (in theory, any
        #  public method could auto-initialize the class)
        def wrapper(self, *args, **kwargs):
            if not self._initialized:
                raise AttributeError("The instance object {} has not yet been "
                                     "initialized. Either call .init() on the "
                                     "object, or decorate the attribute being "
                                     "accessed with @allow_access or "
                                     "@initialize_class. WARNING: Using these "
                                     "decorated methods within __init__ can "
                                     "blow the stack.".format(repr(self)))
            return attr(self, *args, **kwargs)
        return wrapper

    def __new__(mcs, name, bases, attrs):
        # get names of all classes and inherited classes in bases
        mro_list = [kls.__name__ for kls in
                    itertools.chain(*[type.mro(base) for base in bases])]

        # if ClassTaskBase is not in the mro_set, add it now
        #  (prepended to make it a mixin)
        if ClassTask.ClassTaskBase.__name__ not in set(mro_list):
            bases = (ClassTask.ClassTaskBase, ) + bases

        # for now, all ClassTasks are bound
        attrs.pop('bind', None)

        # make sure app is set
        app = attrs.pop('_app', None) or attrs.pop('app', None)
        if not isinstance(app, Proxy) and app is None:
            app = current_app
        attrs['_app'] = app

        # now a standard create of the class object
        clsobj = super(ClassTask, mcs).__new__(mcs, name, bases, attrs)

        # flag that marks if init() has been called on the instance
        clsobj._initialized = False

        # wrap public methods and @properties with _ensure_init
        dont_wrap = ('enqueue', 'init', 'apply_async', 'apply', 'retry',
                     'subtask_from_request')
        for key, value in vars(clsobj).iteritems():
            if not key.startswith('_'):
                if callable(value) and not (hasattr(value, '_auto_init')
                                            or key in dont_wrap
                                            or isinstance(value, staticmethod)
                                            or isinstance(value, classmethod)):
                    setattr(clsobj, key, ClassTask._ensure_init(value))
                elif isinstance(value, property):  # for class @properties
                    setattr(clsobj, key, property(ClassTask._ensure_init(value.__get__),
                                                  value.__set__, value.__delattr__))
        return clsobj

    def __call__(cls, *args, **kwargs):
        instance = cls.__new__(cls, *args, **kwargs)
        instance._args = args      # hold reference to args and kwargs
        instance._kwargs = kwargs  # on class instance
        return instance  # NOTE: __init__ is not called


def auto_init(attr):
    """A decorator for class attributes that guarantees :meth:`__init__` is
    called once (and only once) prior to the attribute being accessed.
    """
    @wraps(attr)
    def wrapper(self, *args, **kwargs):
        if not self._initialized:
            self.init()
        return attr(self, *args, **kwargs)
    wrapper._auto_init = True
    return wrapper
