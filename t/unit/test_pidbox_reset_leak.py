import types

import pytest

from celery.worker import pidbox


class DummyConsumer:
    def __init__(self, name, created, canceled):
        self.name = name
        self._created = created
        self._canceled = canceled
        created.append(self)

    def consume(self):
        pass

    def cancel(self):
        # record cancellations if any
        self._canceled.append(self)


class DummyChannel:
    def __init__(self, closed):
        self.closed = closed

    def close(self):
        self.closed.append(True)


class DummyNode:
    def __init__(self, created, canceled, closed):
        self._created = created
        self._canceled = canceled
        self._closed = closed
        self.channel = DummyChannel(closed)

    def listen(self, callback=None):
        # return a DummyConsumer and keep a reference
        return DummyConsumer('c', self._created, self._canceled)

    def handle_message(self, body, message):
        # Simulate raising an exception so pidbox.reset() is called
        raise RuntimeError('simulated handler error')


class DummyApp:
    def __init__(self, node_cls):
        # emulate control.mailbox.Node factory used by pidbox.Pidbox
        self.control = types.SimpleNamespace(mailbox=types.SimpleNamespace(Node=node_cls))
        # Clock is required by Pidbox.__init__
        self.clock = types.SimpleNamespace(forward=lambda: None)


class DummyController:
    def __init__(self):
        self.use_eventloop = False


class DummyConnection:
    def channel(self):
        return DummyChannel([])


class C:
    pass


def test_pidbox_repeated_reset_creates_consumers_but_does_not_cancel():
    created = []
    canceled = []
    closed = []

    # Create a Node factory that produces DummyNode instances capturing lists
    def node_factory(hostname, handlers=None, state=None, **kwargs):
        return DummyNode(created, canceled, closed)

    app = DummyApp(node_factory)

    # Build a minimal controller-like object expected by Pidbox
    c = types.SimpleNamespace()
    c.hostname = 'test@node'
    c.app = app
    c.controller = DummyController()
    c.connection = DummyConnection()
    c.on_decode_error = None  # handler for decode errors
    c.connection_errors = (Exception,)  # for ignore_errors
    c.channel_errors = (Exception,)  # for ignore_errors

    p = pidbox.Pidbox(c)

    # Start the pidbox which will create the first consumer
    p.start(c)

    # Simulate multiple incoming messages that cause handler errors
    for _ in range(10):
        # on_message wraps node.handle_message and will call reset() on exception
        p.on_message({}, object())

    # After many resets, multiple consumers/channels should have been created
    assert len(created) >= 2
    # With the fix in place, consumers should be canceled during reset
    # to prevent resource leaks (each reset cancels the previous consumer).
    assert len(canceled) >= 1

