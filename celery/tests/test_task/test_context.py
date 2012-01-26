# -*- coding: utf-8 -*-"
from __future__ import absolute_import

import threading

from celery.task.base import Context
from celery.tests.utils import Case


# Retreive the values of all context attributes as a
# dictionary in an implementation-agnostic manner.
def get_context_as_dict(ctx, getter=getattr):
    defaults = {}
    for attr_name in dir(ctx):
        if attr_name.startswith("_"):
            continue   # Ignore pseudo-private attributes
        attr = getter(ctx, attr_name)
        if callable(attr):
            continue   # Ignore methods and other non-trivial types
        defaults[attr_name] = attr
    return defaults
default_context = get_context_as_dict(Context())


# Manipulate the a context in a separate thread
class ContextManipulator(threading.Thread):
    def __init__(self, ctx, *args):
        super(ContextManipulator, self).__init__()
        self.daemon = True
        self.ctx = ctx
        self.args = args
        self.result = None

    def run(self):
        for func, args in self.args:
            func(self.ctx, *args)
        self.result = get_context_as_dict(self.ctx)


class TestTaskContext(Case):

    def test_default_context(self):
        # A bit of a tautological test, since it uses the same
        # initializer as the default_context constructor.
        self.assertDictEqual(get_context_as_dict(Context()), default_context)

    def test_default_context_threaded(self):
        ctx = Context()
        worker = ContextManipulator(ctx)
        worker.start()
        worker.join()
        self.assertDictEqual(worker.result, default_context)
        self.assertDictEqual(get_context_as_dict(ctx), default_context)

    def test_updated_context(self):
        expected = dict(default_context)
        changes = dict(id="unique id", args=["some", 1], wibble="wobble")
        ctx = Context()
        expected.update(changes)
        ctx.update(changes)
        self.assertDictEqual(get_context_as_dict(ctx), expected)
        self.assertDictEqual(get_context_as_dict(Context()), default_context)

    def test_updated_contex_threadedt(self):
        expected_a = dict(default_context)
        changes_a = dict(id="a", args=["some", 1], wibble="wobble")
        expected_a.update(changes_a)
        expected_b = dict(default_context)
        changes_b = dict(id="b", args=["other", 2], weasel="woozle")
        expected_b.update(changes_b)
        ctx = Context()

        worker_a = ContextManipulator(ctx, (Context.update, [changes_a]))
        worker_b = ContextManipulator(ctx, (Context.update, [changes_b]))
        worker_a.start()
        worker_b.start()
        worker_a.join()
        worker_b.join()

        self.assertDictEqual(worker_a.result, expected_a)
        self.assertDictEqual(worker_b.result, expected_b)
        self.assertDictEqual(get_context_as_dict(ctx), default_context)

    def test_modified_context(self):
        expected = dict(default_context)
        ctx = Context()
        expected["id"] = "unique id"
        expected["args"] = ["some", 1]
        ctx.id = "unique id"
        ctx.args = ["some", 1]
        self.assertDictEqual(get_context_as_dict(ctx), expected)
        self.assertDictEqual(get_context_as_dict(Context()), default_context)

    def test_modified_contex_threadedt(self):
        expected_a = dict(default_context)
        expected_a["id"] = "a"
        expected_a["args"] = ["some", 1]
        expected_a["wibble"] = "wobble"
        expected_b = dict(default_context)
        expected_b["id"] = "b"
        expected_b["args"] = ["other", 2]
        expected_b["weasel"] = "woozle"
        ctx = Context()

        worker_a = ContextManipulator(ctx,
                                      (setattr, ["id", "a"]),
                                      (setattr, ["args", ["some", 1]]),
                                      (setattr, ["wibble", "wobble"]))
        worker_b = ContextManipulator(ctx,
                                      (setattr, ["id", "b"]),
                                      (setattr, ["args", ["other", 2]]),
                                      (setattr, ["weasel", "woozle"]))
        worker_a.start()
        worker_b.start()
        worker_a.join()
        worker_b.join()

        self.assertDictEqual(worker_a.result, expected_a)
        self.assertDictEqual(worker_b.result, expected_b)
        self.assertDictEqual(get_context_as_dict(ctx), default_context)

    def test_cleared_context(self):
        changes = dict(id="unique id", args=["some", 1], wibble="wobble")
        ctx = Context()
        ctx.update(changes)
        ctx.clear()
        self.assertDictEqual(get_context_as_dict(ctx), default_context)
        self.assertDictEqual(get_context_as_dict(Context()), default_context)

    def test_cleared_context_threaded(self):
        changes_a = dict(id="a", args=["some", 1], wibble="wobble")
        expected_b = dict(default_context)
        changes_b = dict(id="b", args=["other", 2], weasel="woozle")
        expected_b.update(changes_b)
        ctx = Context()

        worker_a = ContextManipulator(ctx,
                                      (Context.update, [changes_a]),
                                      (Context.clear, []))
        worker_b = ContextManipulator(ctx,
                                      (Context.update, [changes_b]))
        worker_a.start()
        worker_b.start()
        worker_a.join()
        worker_b.join()

        self.assertDictEqual(worker_a.result, default_context)
        self.assertDictEqual(worker_b.result, expected_b)
        self.assertDictEqual(get_context_as_dict(ctx), default_context)

    def test_context_get(self):
        expected = dict(default_context)
        changes = dict(id="unique id", args=["some", 1], wibble="wobble")
        ctx = Context()
        expected.update(changes)
        ctx.update(changes)
        ctx_dict = get_context_as_dict(ctx, getter=Context.get)
        self.assertDictEqual(ctx_dict, expected)
        self.assertDictEqual(get_context_as_dict(Context()), default_context)
