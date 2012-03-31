from __future__ import absolute_import

import sys

from types import ModuleType

from .local import Proxy

compat_modules = {
    "messaging": {
        "TaskPublisher": "amqp.TaskPublisher",
        "ConsumerSet": "amqp.ConsumerSet",
        "TaskConsumer": "amqp.TaskConsumer",
        "establish_connection": "broker_connection",
        "with_connection": "with_default_connection",
        "get_consumer_set": "amqp.get_task_consumer"
    },
    "log": {
        "get_default_logger": "log.get_default_logger",
        "setup_logger": "log.setup_logger",
        "setup_task_logger": "log.setup_task_logger",
        "get_task_logger": "log.get_task_logger",
        "setup_loggig_subsystem": "log.setup_logging_subsystem",
        "redirect_stdouts_to_logger": "log.redirect_stdouts_to_logger",
    },
}

def rgetattr(obj, path):
    for part in path:
        obj = getattr(obj, part)
    return obj

def _module(g, name, attrs):
    attrs = dict((name, Proxy(rgetattr, (g.current_app, attr.split('.'))))
                    for name, attr in attrs.iteritems())
    return type(name, (ModuleType, ), attrs)('.'.join(["celery", name]))


def install_compat_modules(g):
    from types import ModuleType
    mods = sys.modules

    current_app = g.current_app

    for name, attrs in compat_modules.iteritems():
        print("CREATE MODULE: %r %r" % (name, attrs))
        module = _module(g, name, attrs)
        setattr(g, name, module)
        sys.modules[module.__name__] = module

    class registry(ModuleType):
        tasks = Proxy(lambda: current_app.tasks)
    g.registry = mods["celery.registry"] = registry("celery.registry")

    class decorators(ModuleType):
        def task(*args, **kwargs):
            kwargs.setdefault("accept_magic_kwargs", True)
            return current_app.task(*args, **kwargs)

        def periodic_task(*args, **kwargs):
            from celery.task import periodic_task
            kwargs.setdefault("accept_magic_kwargs", True)
            return periodic_task(*args, **kwargs)
    g.decorators = mods["celery.decorators"] \
            = decorators("celery.decorators")
