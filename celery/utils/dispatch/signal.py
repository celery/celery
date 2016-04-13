# -*- coding: utf-8 -*-
"""Signal class."""
from __future__ import absolute_import, unicode_literals

import weakref

from celery.five import python_2_unicode_compatible, range, text_t
from celery.local import PromiseProxy, Proxy
from celery.utils.log import get_logger

from . import saferef

__all__ = ['Signal']

logger = get_logger(__name__)

WEAKREF_TYPES = (weakref.ReferenceType, saferef.BoundMethodWeakref)


def _make_id(target):  # pragma: no cover
    if isinstance(target, Proxy):
        target = target._get_current_object()
    if isinstance(target, (bytes, text_t)):
        # see Issue #2475
        return target
    if hasattr(target, '__func__'):
        return (id(target.__self__), id(target.__func__))
    return id(target)


@python_2_unicode_compatible
class Signal(object):  # pragma: no cover
    """Observer pattern implementation.

    :param providing_args: A list of the arguments this signal can pass
        along in a :meth:`send` call.

    """

    #: Holds a dictionary of
    #: ``{receiverkey (id): weakref(receiver)}`` mappings.
    receivers = None

    def __init__(self, providing_args=None):
        self.receivers = []
        if providing_args is None:
            providing_args = []
        self.providing_args = set(providing_args)

    def _connect_proxy(self, fun, sender, weak, dispatch_uid):
        return self.connect(
            fun, sender=sender._get_current_object(),
            weak=weak, dispatch_uid=dispatch_uid,
        )

    def connect(self, *args, **kwargs):
        """Connect receiver to sender for signal.

        :param receiver: A function or an instance method which is to
            receive signals. Receivers must be hashable objects.

            if weak is :const:`True`, then receiver must be weak-referencable
            (more precisely :func:`saferef.safe_ref()` must be able to create a
            reference to the receiver).

            Receivers must be able to accept keyword arguments.

            If receivers have a `dispatch_uid` attribute, the receiver will
            not be added if another receiver already exists with that
            `dispatch_uid`.

        :keyword sender: The sender to which the receiver should respond.
            Must either be of type :class:`Signal`, or :const:`None` to receive
            events from any sender.

        :keyword weak: Whether to use weak references to the receiver.
            By default, the module will attempt to use weak references to the
            receiver objects. If this parameter is false, then strong
            references will be used.

        :keyword dispatch_uid: An identifier used to uniquely identify a
            particular instance of a receiver. This will usually be a
            string, though it may be anything hashable.

        """
        def _handle_options(sender=None, weak=True, dispatch_uid=None):

            def _connect_signal(fun):
                receiver = fun

                if isinstance(sender, PromiseProxy):
                    sender.__then__(
                        self._connect_proxy, fun, sender, weak, dispatch_uid,
                    )
                    return fun

                if dispatch_uid:
                    lookup_key = (dispatch_uid, _make_id(sender))
                else:
                    lookup_key = (_make_id(receiver), _make_id(sender))

                if weak:
                    receiver = saferef.safe_ref(
                        receiver, on_delete=self._remove_receiver,
                    )

                for r_key, _ in self.receivers:
                    if r_key == lookup_key:
                        break
                else:
                    self.receivers.append((lookup_key, receiver))

                return fun

            return _connect_signal

        if args and callable(args[0]):
            return _handle_options(*args[1:], **kwargs)(args[0])
        return _handle_options(*args, **kwargs)

    def disconnect(self, receiver=None, sender=None, weak=True,
                   dispatch_uid=None):
        """Disconnect receiver from sender for signal.

        If weak references are used, disconnect need not be called. The
        receiver will be removed from dispatch automatically.

        :keyword receiver: The registered receiver to disconnect. May be
            none if `dispatch_uid` is specified.

        :keyword sender: The registered sender to disconnect.

        :keyword weak: The weakref state to disconnect.

        :keyword dispatch_uid: the unique identifier of the receiver
            to disconnect

        """
        if dispatch_uid:
            lookup_key = (dispatch_uid, _make_id(sender))
        else:
            lookup_key = (_make_id(receiver), _make_id(sender))

        for index in range(len(self.receivers)):
            (r_key, _) = self.receivers[index]
            if r_key == lookup_key:
                del self.receivers[index]
                break

    def send(self, sender, **named):
        """Send signal from sender to all connected receivers.

        If any receiver raises an error, the error propagates back through
        send, terminating the dispatch loop, so it is quite possible to not
        have all receivers called if a raises an error.

        :param sender: The sender of the signal. Either a specific
            object or :const:`None`.

        :keyword \*\*named: Named arguments which will be passed to receivers.

        :returns: a list of tuple pairs: `[(receiver, response), … ]`.

        """
        responses = []
        if not self.receivers:
            return responses

        for receiver in self._live_receivers(_make_id(sender)):
            try:
                response = receiver(signal=self, sender=sender, **named)
            except Exception as exc:
                logger.error('Signal handler %r raised: %r',
                             receiver, exc, exc_info=1)
            else:
                responses.append((receiver, response))
        return responses

    def _live_receivers(self, senderkey):
        """Filter sequence of receivers to get resolved, live receivers.

        This checks for weak references and resolves them, then returning only
        live receivers.

        """
        none_senderkey = _make_id(None)
        receivers = []

        for (receiverkey, r_senderkey), receiver in self.receivers:
            if r_senderkey == none_senderkey or r_senderkey == senderkey:
                if isinstance(receiver, WEAKREF_TYPES):
                    # Dereference the weak reference.
                    receiver = receiver()
                    if receiver is not None:
                        receivers.append(receiver)
                else:
                    receivers.append(receiver)
        return receivers

    def _remove_receiver(self, receiver):
        """Remove dead receivers from connections."""

        to_remove = []
        for key, connected_receiver in self.receivers:
            if connected_receiver == receiver:
                to_remove.append(key)
        for key in to_remove:
            for idx, (r_key, _) in enumerate(self.receivers):
                if r_key == key:
                    del self.receivers[idx]

    def __repr__(self):
        return '<Signal: {0}>'.format(type(self).__name__)

    def __str__(self):
        return repr(self)
