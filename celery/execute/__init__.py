# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .. import current_app
from ..utils import deprecated

send_task = current_app.send_task


@deprecated(removal="2.3", alternative="Use task.apply_async() instead.")
def apply_async(task, *args, **kwargs):
    """*[Deprecated]* Use `task.apply_async()`"""
    return task.apply_async(*args, **kwargs)


@deprecated(removal="2.3", alternative="Use task.apply() instead.")
def apply(task, *args, **kwargs):
    """*[Deprecated]* Use `task.apply()`"""
    return task.apply(*args, **kwargs)


@deprecated(removal="2.3",
            alternative="Use registry.tasks[name].delay instead.")
def delay_task(task, *args, **kwargs):
    from ..registry import tasks
    return tasks[task].apply_async(args, kwargs)
