import abc
from typing import Mapping


class AppT(metaclass=abc.ABCMeta):
    ...


class TaskT(metaclass=abc.ABCMeta):
    ...


class SignatureT(metaclass=abc.ABCMeta):
    ...


class BlueprintT(metaclass=abc.ABCMeta):
    ...


class BootstepT(metaclass=abc.ABCMeta):
    ...


class WorkerConsumerT(metaclass=abc.ABCMeta):
    ...


class WorkerT(metaclass=abc.ABCMeta):
    ...


class RouterT(metaclass=abc.ABCMeta):
    ...


class TracerT(metaclass=abc.ABCMeta):
    ...


class BeatT(metaclass=abc.ABCMeta):
    ...


class PoolT(metaclass=abc.ABCMeta):
    ...


class AutoscalerT(metaclass=abc.ABCMeta):
    ...


class SignalT(metaclass=abc.ABCMeta):
    ...


class TimerT(metaclass=abc.ABCMeta):
    ...


class RequestT(metaclass=abc.ABCMeta):
    ...


class TaskRegistryT(metaclass=abc.ABCMeta):
    ...


class ControlStateT(metaclass=abc.ABCMeta):
    ...


class ResultT(metaclass=abc.ABCMeta):
    ...


class AppAMQPT(metaclass=abc.ABCMeta):
    ...


class AppControlT(metaclass=abc.ABCMeta):
    ...


class AppEventsT(metaclass=abc.ABCMeta):
    ...


class AppLogT(metaclass=abc.ABCMeta):
    ...


class LoaderT(metaclass=abc.ABCMeta):
    ...


EventT = Mapping
