# -*- coding: utf-8 -*-
"""Worker name utilities."""
import os
import socket

from functools import partial
from typing import Dict, Optional, Tuple

from kombu.entity import Exchange, Queue

from .functional import memoize
from .text import simple_format

#: Exchange for worker direct queues.
WORKER_DIRECT_EXCHANGE = Exchange('C.dq2')

#: Format for worker direct queue names.
WORKER_DIRECT_QUEUE_FORMAT = '{hostname}.dq2'

#: Separator for worker node name and hostname.
NODENAME_SEP = '@'

NODENAME_DEFAULT = 'celery'

gethostname = memoize(1, Cache=dict)(socket.gethostname)

__all__ = [
    'worker_direct', 'gethostname', 'nodename',
    'anon_nodename', 'nodesplit', 'default_nodename',
    'node_format', 'host_format',
]


def worker_direct(hostname: str) -> Queue:
    """Return the :class:`kombu.Queue` being a direct route to a worker.

    Arguments:
        hostname (str, ~kombu.Queue): The fully qualified node name of
            a worker (e.g., ``w1@example.com``).  If passed a
            :class:`kombu.Queue` instance it will simply return
            that instead.
    """
    if isinstance(hostname, Queue):
        return hostname
    return Queue(
        WORKER_DIRECT_QUEUE_FORMAT.format(hostname=hostname),
        WORKER_DIRECT_EXCHANGE,
        hostname,
    )


def nodename(name: str, hostname: str) -> str:
    """Create node name from name/hostname pair."""
    return NODENAME_SEP.join((name, hostname))


def anon_nodename(hostname: Optional[str]=None, prefix: str='gen') -> str:
    """Return the nodename for this process (not a worker).

    This is used for e.g. the origin task message field.
    """
    return nodename(''.join([prefix, str(os.getpid())]),
                    hostname or gethostname())


def nodesplit(name: str) -> Tuple[str]:
    """Split node name into tuple of name/hostname."""
    parts = name.split(NODENAME_SEP, 1)
    if len(parts) == 1:
        return None, parts[0]
    return parts


def default_nodename(hostname: str) -> str:
    """Return the default nodename for this process."""
    name, host = nodesplit(hostname or '')
    return nodename(name or NODENAME_DEFAULT, host or gethostname())


def node_format(s: str, name: str, **extra: Dict[str, str]) -> str:
    """Format worker node name (name@host.com)."""
    shortname, host = nodesplit(name)
    return host_format(
        s, host, shortname or NODENAME_DEFAULT, p=name, **extra)


def _fmt_process_index(prefix: str='', default: str='0') -> str:
    from .log import current_process_index
    index = current_process_index()
    return '{0}{1}'.format(prefix, index) if index else default
_fmt_process_index_with_prefix = partial(_fmt_process_index, '-', '')


def host_format(s: str,
                host: Optional[str]=None, name: Optional[str]=None,
                **extra: Dict[str, str]) -> str:
    """Format host %x abbreviations."""
    host = host or gethostname()
    hname, _, domain = host.partition('.')
    name = name or hname
    keys = dict({
        'h': host, 'n': name, 'd': domain,
        'i': _fmt_process_index, 'I': _fmt_process_index_with_prefix,
    }, **extra)
    return simple_format(s, keys)
