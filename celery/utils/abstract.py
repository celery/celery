# -*- coding: utf-8 -*-
"""Abstract classes."""
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import Callable

from typing import Any, Sequence, Tuple

__all__ = ['CallableTask', 'CallableSignature']


def _hasattr(C, attr):
    return any(attr in B.__dict__ for B in C.__mro__)


class _AbstractClass(metaclass=ABCMeta):
    __required_attributes__ = frozenset()

    @classmethod
    def _subclasshook_using(cls, parent, C):
        return (
            cls is parent and
            all(_hasattr(C, attr) for attr in cls.__required_attributes__)
        ) or NotImplemented

    @classmethod
    def register(cls, other: Any):
        # we override `register` to return other for use as a decorator.
        type(cls).register(cls, other)
        return other


class AbstractApp(_AbstractClass):  # pragma: no cover
    __required_attributes = frozenset({
        'close' 'start', 'task', 'AsyncResult', 'finalize',
    })

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def task(self) -> 'CallableTask':
        pass

    @abstractmethod
    def finalize(self):
        pass

    @abstractproperty
    def conf(self):
        pass

    @abstractproperty
    def AsyncResult(self):
        pass


class AbstractResult(_AbstractClass):  # pragma: no cover
    __required_attributes__ = frozenset({
        'as_tuple', 'forget', 'get', 'ready', 'successful', 'failed',
    })

    @abstractmethod
    def as_tuple(self) -> Tuple:
        pass

    @abstractmethod
    def forget(self) -> None:
        pass

    @abstractmethod
    def get(self, *args, **kwargs) -> Any:
        pass

    @abstractmethod
    def ready(self) -> bool:
        pass

    @abstractmethod
    def successful(self) -> bool:
        pass

    @abstractmethod
    def failed(self) -> bool:
        pass

    @abstractproperty
    def backend(self) -> Any:
        pass

    @abstractproperty
    def children(self) -> Sequence['AbstractResult']:
        pass

    @abstractproperty
    def result(self) -> Any:
        pass

    @abstractproperty
    def traceback(self) -> str:
        pass

    @abstractproperty
    def state(self) -> str:
        pass


class CallableTask(_AbstractClass, Callable):  # pragma: no cover
    __required_attributes__ = frozenset({
        'delay', 'apply_async', 'apply',
    })

    @abstractmethod
    def delay(self, *args, **kwargs):
        pass

    @abstractmethod
    def apply_async(self, *args, **kwargs):
        pass

    @abstractmethod
    def apply(self, *args, **kwargs):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        return cls._subclasshook_using(CallableTask, C)


class CallableSignature(CallableTask):  # pragma: no cover
    __required_attributes__ = frozenset({
        'clone', 'freeze', 'set', 'link', 'link_error', '__or__',
    })

    @abstractproperty
    def name(self):
        pass

    @abstractproperty
    def type(self):
        pass

    @abstractproperty
    def app(self):
        pass

    @abstractproperty
    def id(self):
        pass

    @abstractproperty
    def task(self):
        pass

    @abstractproperty
    def args(self):
        pass

    @abstractproperty
    def kwargs(self):
        pass

    @abstractproperty
    def options(self):
        pass

    @abstractproperty
    def subtask_type(self):
        pass

    @abstractproperty
    def chord_size(self):
        pass

    @abstractproperty
    def immutable(self):
        pass

    @abstractmethod
    def clone(self, args=None, kwargs=None):
        pass

    @abstractmethod
    def freeze(self, id: str=None, group_id: str=None,
               chord: str=None, root_id: str=None) -> AbstractResult:
        pass

    @abstractmethod
    def set(self, immutable: bool=None, **options) -> 'CallableSignature':
        pass

    @abstractmethod
    def link(self, callback: 'CallableSignature') -> 'CallableSignature':
        pass

    @abstractmethod
    def link_error(self, errback: 'CallableSignature') -> 'CallableSignature':
        pass

    @abstractmethod
    def __or__(self, other: 'CallableSignature') -> 'CallableSignature':
        pass

    @abstractmethod
    def __invert__(self) -> Any:
        pass

    @classmethod
    def __subclasshook__(cls, C: Any) -> Any:
        return cls._subclasshook_using(CallableSignature, C)
