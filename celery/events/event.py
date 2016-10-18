"""Creating events, and event exchange definition."""
from __future__ import absolute_import, unicode_literals
import time
from copy import copy
from kombu import Exchange

__all__ = [
    'Event', 'event_exchange', 'get_exchange', 'group_from',
]

#: Exchange used to send events on.
#: Note: Use :func:`get_exchange` instead, as the type of
#: exchange will vary depending on the broker connection.
event_exchange = Exchange('celeryev', type='topic')


def Event(type, _fields=None, __dict__=dict, __now__=time.time, **fields):
    """Create an event.

    Notes:
        An event is simply a dictionary: the only required field is ``type``.
        A ``timestamp`` field will be set to the current time if not provided.
    """
    event = __dict__(_fields, **fields) if _fields else fields
    if 'timestamp' not in event:
        event.update(timestamp=__now__(), type=type)
    else:
        event['type'] = type
    return event


def group_from(type):
    """Get the group part of an event type name.

    Example:
        >>> group_from('task-sent')
        'task'

        >>> group_from('custom-my-event')
        'custom'
    """
    return type.split('-', 1)[0]


def get_exchange(conn):
    """Get exchange used for sending events.

    Arguments:
        conn (kombu.Connection): Connection used for sending/receving events.

    Note:
        The event type changes if Redis is used as the transport
        (from topic -> fanout).
    """
    ex = copy(event_exchange)
    if conn.transport.driver_type == 'redis':
        # quick hack for Issue #436
        ex.type = 'fanout'
    return ex
