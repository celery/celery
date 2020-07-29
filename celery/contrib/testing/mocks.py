"""Useful mocks for unit testing."""
from __future__ import absolute_import, unicode_literals

import numbers
from datetime import datetime, timedelta

try:
    from case import Mock
except ImportError:
    try:
        from unittest.mock import Mock
    except ImportError:
        from mock import Mock


def TaskMessage(
    name,  # type: str
    id=None,  # type: str
    args=(),  # type: Sequence
    kwargs=None,  # type: Mapping
    callbacks=None,  # type: Sequence[Signature]
    errbacks=None,  # type: Sequence[Signature]
    chain=None,  # type: Sequence[Signature]
    shadow=None,  # type: str
    utc=None,  # type: bool
    **options  # type: Any
):
    # type: (...) -> Any
    """Create task message in protocol 2 format."""
    kwargs = {} if not kwargs else kwargs
    from celery import uuid
    from kombu.serialization import dumps
    id = id or uuid()
    message = Mock(name='TaskMessage-{0}'.format(id))
    message.headers = {
        'id': id,
        'task': name,
        'shadow': shadow,
    }
    embed = {'callbacks': callbacks, 'errbacks': errbacks, 'chain': chain}
    message.headers.update(options)
    message.content_type, message.content_encoding, message.body = dumps(
        (args, kwargs, embed), serializer='json',
    )
    message.payload = (args, kwargs, embed)
    return message


def TaskMessage1(
    name,  # type: str
    id=None,  # type: str
    args=(),  # type: Sequence
    kwargs=None,  # type: Mapping
    callbacks=None,  # type: Sequence[Signature]
    errbacks=None,  # type: Sequence[Signature]
    chain=None,  # type: Squence[Signature]
    **options  # type: Any
):
    # type: (...) -> Any
    """Create task message in protocol 1 format."""
    kwargs = {} if not kwargs else kwargs
    from celery import uuid
    from kombu.serialization import dumps
    id = id or uuid()
    message = Mock(name='TaskMessage-{0}'.format(id))
    message.headers = {}
    message.payload = {
        'task': name,
        'id': id,
        'args': args,
        'kwargs': kwargs,
        'callbacks': callbacks,
        'errbacks': errbacks,
    }
    message.payload.update(options)
    message.content_type, message.content_encoding, message.body = dumps(
        message.payload,
    )
    return message


def task_message_from_sig(app, sig, utc=True, TaskMessage=TaskMessage):
    # type: (Celery, Signature, bool, Any) -> Any
    """Create task message from :class:`celery.Signature`.

    Example:
        >>> m = task_message_from_sig(app, add.s(2, 2))
        >>> amqp_client.basic_publish(m, exchange='ex', routing_key='rkey')
    """
    sig.freeze()
    callbacks = sig.options.pop('link', None)
    errbacks = sig.options.pop('link_error', None)
    countdown = sig.options.pop('countdown', None)
    if countdown:
        eta = app.now() + timedelta(seconds=countdown)
    else:
        eta = sig.options.pop('eta', None)
    if eta and isinstance(eta, datetime):
        eta = eta.isoformat()
    expires = sig.options.pop('expires', None)
    if expires and isinstance(expires, numbers.Real):
        expires = app.now() + timedelta(seconds=expires)
    if expires and isinstance(expires, datetime):
        expires = expires.isoformat()
    return TaskMessage(
        sig.task, id=sig.id, args=sig.args,
        kwargs=sig.kwargs,
        callbacks=[dict(s) for s in callbacks] if callbacks else None,
        errbacks=[dict(s) for s in errbacks] if errbacks else None,
        eta=eta,
        expires=expires,
        utc=utc,
        **sig.options
    )
