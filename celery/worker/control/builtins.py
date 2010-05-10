from datetime import datetime

from celery.registry import tasks
from celery.worker.revoke import revoked
from celery.worker.control.registry import Panel

TASK_INFO_FIELDS = ("exchange", "routing_key", "rate_limit")


@Panel.register
def revoke(panel, task_id, **kwargs):
    """Revoke task by task id."""
    revoked.add(task_id)
    panel.logger.warn("Task %s revoked" % (task_id, ))
    return True


@Panel.register
def rate_limit(panel, task_name, rate_limit, **kwargs):
    """Set new rate limit for a task type.

    See :attr:`celery.task.base.Task.rate_limit`.

    :param task_name: Type of task.
    :param rate_limit: New rate limit.

    """
    try:
        tasks[task_name].rate_limit = rate_limit
    except KeyError:
        panel.logger.error("Rate limit attempt for unknown task %s" % (
            task_name, ))
        return "unknown task"

    panel.listener.ready_queue.refresh()

    if not rate_limit:
        panel.logger.warn("Disabled rate limits for tasks of type %s" % (
                            task_name, ))
    else:
        panel.logger.warn("New rate limit for tasks of type %s: %s." % (
                    task_name, rate_limit))
    return True


@Panel.register
def dump_schedule(panel, **kwargs):
    schedule = panel.listener.eta_schedule
    info = ["--Empty Schedule--"]
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
