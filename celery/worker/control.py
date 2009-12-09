from celery.worker.revoke import revoked
from celery.registry import tasks


def expose(fun):
    fun.exposed = True
    return fun


class Control(object):

    def __init__(self, logger):
        self.logger = logger

    @expose
    def revoke(self, task_id, **kwargs):
        revoked.add(task_id)
        self.logger.warn("Task %s revoked." % task_id)

    @expose
    def rate_limit(self, task_name, rate_limit):
        try:
            tasks[task_name].rate_limit = rate_limit
        except KeyError:
            return

        if not rate_limit:
            self.logger.warn("Disabled rate limits for tasks of type %s" % (
                                task_name))
        else:
            self.logger.warn("New rate limit for tasks of type %s: %s." % (
                                task_name, rate_limit))


class ControlDispatch(object):
    panel_cls = Control

    def __init__(self, logger):
        self.logger = logger
        self.panel = self.panel_cls(self.logger)

    def dispatch(self, command, kwargs):
        control = None
        try:
            control = getattr(self.panel, command)
        except AttributeError:
            pass
        if control is None or not control.exposed:
            self.logger.error("No such control command: %s" % command)
        else:
            return control(**kwargs)
