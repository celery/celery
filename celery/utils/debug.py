# -*- coding: utf-8 -*-
"""Utilities for debugging memory usage, blocking calls, etc."""
import os
import sys
import traceback

from contextlib import contextmanager
from functools import partial
from io import StringIO
from pprint import pprint
from typing import (
    Any, AnyStr, Generator, IO, Iterator, Iterable,
    Optional, Sequence, SupportsInt, Tuple, Union,
)
from typing import MutableSequence  # noqa

from celery.platforms import signals

from .typing import Timeout

try:
    from psutil import Process
except ImportError:
    class Process:  # noqa
        pass

__all__ = [
    'blockdetection', 'sample_mem', 'memdump', 'sample',
    'humanbytes', 'mem_rss', 'ps', 'cry',
]

UNITS = (               # type: Sequence[Tuple[float, str]]
    (2 ** 40.0, 'TB'),
    (2 ** 30.0, 'GB'),
    (2 ** 20.0, 'MB'),
    (2 ** 10.0, 'KB'),
    (0.0, 'b'),
)

_process = None         # type: Optional[Process]
_mem_sample = []        # type: MutableSequence[str]


def _on_blocking(signum: int, frame: Any) -> None:
    import inspect
    raise RuntimeError(
        'Blocking detection timed-out at: {0}'.format(
            inspect.getframeinfo(frame)
        )
    )


@contextmanager
def blockdetection(timeout: Timeout) -> Generator:
    """Context that raises an exception if process is blocking.

    Uses ``SIGALRM`` to detect blocking functions.
    """
    if not timeout:
        yield
    else:
        old_handler = signals['ALRM']
        old_handler = None if old_handler == _on_blocking else old_handler

        signals['ALRM'] = _on_blocking

        try:
            yield signals.arm_alarm(timeout)
        finally:
            if old_handler:
                signals['ALRM'] = old_handler
            signals.reset_alarm()


def sample_mem() -> str:
    """Sample RSS memory usage.

    Statistics can then be output by calling :func:`memdump`.
    """
    current_rss = mem_rss()
    _mem_sample.append(current_rss)
    return current_rss


def _memdump(samples: int=10) -> Tuple[Iterable[Any], str]:  # pragma: no cover
    S = _mem_sample
    prev = list(S) if len(S) <= samples else sample(S, samples)
    _mem_sample[:] = []
    import gc
    gc.collect()
    after_collect = mem_rss()
    return prev, after_collect


def memdump(samples: int=10, file: IO=None) -> None:  # pragma: no cover
    """Dump memory statistics.

    Will print a sample of all RSS memory samples added by
    calling :func:`sample_mem`, and in addition print
    used RSS memory after :func:`gc.collect`.
    """
    say = partial(print, file=file)
    if ps() is None:
        say('- rss: (psutil not installed).')
        return
    prev, after_collect = _memdump(samples)
    if prev:
        say('- rss (sample):')
        for mem in prev:
            say('-    > {0},'.format(mem))
    say('- rss (end): {0}.'.format(after_collect))


def sample(x: Sequence, n: int, k: int=0) -> Iterator[Any]:
    """Given a list `x` a sample of length ``n`` of that list is returned.

    For example, if `n` is 10, and `x` has 100 items, a list of every tenth.
    item is returned.

    ``k`` can be used as offset.
    """
    j = len(x) // n
    for _ in range(n):
        try:
            yield x[k]
        except IndexError:
            break
        k += j


def hfloat(f: Union[SupportsInt, AnyStr], p: int=5) -> str:
    """Convert float to value suitable for humans.

    Arguments:
        f (float): The floating point number.
        p (int): Floating point precision (default is 5).
    """
    i = int(f)
    return str(i) if i == f else '{0:.{p}}'.format(f, p=p)


def humanbytes(s: Union[float, int]) -> str:
    """Convert bytes to human-readable form (e.g., KB, MB)."""
    return next(
        '{0}{1}'.format(hfloat(s / div if div else s), unit)
        for div, unit in UNITS if s >= div
    )


def mem_rss() -> str:
    """Return RSS memory usage as a humanized string."""
    p = ps()
    if p is not None:
        return humanbytes(_process_memory_info(p).rss)


def ps() -> Process:  # pragma: no cover
    """Return the global :class:`psutil.Process` instance.

    Note:
        Returns :const:`None` if :pypi:`psutil` is not installed.
    """
    global _process
    if _process is None and Process is not None:
        _process = Process(os.getpid())
    return _process


def _process_memory_info(process: Process) -> Any:
    try:
        return process.memory_info()
    except AttributeError:
        return process.get_memory_info()


def cry(out: Optional[StringIO]=None,
        sepchr: str='=', seplen: int=49) -> None:  # pragma: no cover
    """Return stack-trace of all active threads.

    See Also:
        Taken from https://gist.github.com/737056.
    """
    import threading

    out = StringIO() if out is None else out
    P = partial(print, file=out)

    # get a map of threads by their ID so we can print their names
    # during the traceback dump
    tmap = {t.ident: t for t in threading.enumerate()}

    sep = sepchr * seplen
    for tid, frame in sys._current_frames().items():
        thread = tmap.get(tid)
        if not thread:
            # skip old junk (left-overs from a fork)
            continue
        P('{0.name}'.format(thread))
        P(sep)
        traceback.print_stack(frame, file=out)
        P(sep)
        P('LOCAL VARIABLES')
        P(sep)
        pprint(frame.f_locals, stream=out)
        P('\n')
    return out.getvalue()
