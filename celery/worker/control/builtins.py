from datetime import datetime

from celery import conf
from celery.backends import default_backend
from celery.registry import tasks
from celery.utils import timeutils
from celery.worker import state
from celery.worker.revoke import revoked
from celery.worker.control.registry import Panel

TASK_INFO_FIELDS = ("exchange", "routing_key", "rate_limit")


@Panel.register
def revoke(panel, task_id, task_name=None, **kwargs):
    """Revoke task by task id."""
    revoked.add(task_id)
    backend = default_backend
    if task_name: # Use custom task backend (if any)
        try:
            backend = tasks[task_name].backend
        except KeyError:
            pass
    backend.mark_as_revoked(task_id)
    panel.logger.warn("Task %s revoked" % (task_id, ))
    return {"ok": "task %s revoked" % (task_id, )}


@Panel.register
def enable_events(panel):
    dispatcher = panel.listener.event_dispatcher
    if not dispatcher.enabled:
        dispatcher.enable()
        dispatcher.send("worker-online")
        panel.logger.warn("Events enabled by remote.")
        return {"ok": "events enabled"}
    return {"ok": "events already enabled"}


@Panel.register
def disable_events(panel):
    dispatcher = panel.listener.event_dispatcher
    if dispatcher.enabled:
        dispatcher.send("worker-offline")
        dispatcher.disable()
        panel.logger.warn("Events disabled by remote.")
        return {"ok": "events disabled"}
    return {"ok": "events already disabled"}


@Panel.register
def rate_limit(panel, task_name, rate_limit, **kwargs):
    """Set new rate limit for a task type.

    See :attr:`celery.task.base.Task.rate_limit`.

    :param task_name: Type of task.
    :param rate_limit: New rate limit.

    """

    try:
        timeutils.rate(rate_limit)
    except ValueError, exc:
        return {"error": "Invalid rate limit string: %s" % exc}

    try:
        tasks[task_name].rate_limit = rate_limit
    except KeyError:
        panel.logger.error("Rate limit attempt for unknown task %s" % (
            task_name, ))
        return {"error": "unknown task"}

    if conf.DISABLE_RATE_LIMITS:
        panel.logger.error("Rate limit attempt, but rate limits disabled.")
        return {"error": "rate limits disabled"}

    panel.listener.ready_queue.refresh()

    if not rate_limit:
        panel.logger.warn("Disabled rate limits for tasks of type %s" % (
                            task_name, ))
        return {"ok": "rate limit disabled successfully"}

    panel.logger.warn("New rate limit for tasks of type %s: %s." % (
                task_name, rate_limit))
    return {"ok": "new rate limit set successfully"}


@Panel.register
def dump_schedule(panel, **kwargs):
    schedule = panel.listener.eta_schedule
    if not schedule.queue:
        panel.logger.info("--Empty schedule--")
        return []

    formatitem = lambda (i, item): "%s. %s pri%s %r" % (i,
            datetime.fromtimestamp(item["eta"]),
            item["priority"],
            item["item"])
    info = map(formatitem, enumerate(schedule.info()))
    panel.logger.info("* Dump of current schedule:\n%s" % (
                            "\n".join(info, )))
    return info


@Panel.register
def dump_reserved(panel, **kwargs):
    ready_queue = panel.listener.ready_queue
    reserved = ready_queue.items
    if not reserved:
        panel.logger.info("--Empty queue--")
        return []
    info = map(repr, reserved)
    panel.logger.info("* Dump of currently reserved tasks:\n%s" % (
                            "\n".join(info, )))
    return info


@Panel.register
def dump_active(panel, **kwargs):
    from celery.worker.state import active
    return active


@Panel.register
def stats(panel, **kwargs):
    return {"active": state.active,
            "total": state.total,
            "pool": panel.listener.pool.info}


@Panel.register
def dump_tasks(panel, **kwargs):

    def _extract_info(task):
        fields = dict((field, str(getattr(task, field, None)))
                        for field in TASK_INFO_FIELDS
                            if getattr(task, field, None) is not None)
        info = map("=".join, fields.items())
        if not info:
            return task.name
        return "%s [%s]" % (task.name, " ".join(info))

    info = map(_extract_info, (tasks[task]
                                        for task in sorted(tasks.keys())))
    panel.logger.warn("* Dump of currently registered tasks:\n%s" % (
                "\n".join(info)))

    return info


@Panel.register
def ping(panel, **kwargs):
    return "pong"


@Panel.register
def shutdown(panel, **kwargs):
    panel.logger.critical("Got shutdown from remote.")
    raise SystemExit
