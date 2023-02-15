from time import sleep

from config import app
from visitors import FullVisitor, MonitoringIdStampingVisitor, MyStampingVisitor

from celery import Task
from celery.canvas import Signature, maybe_signature
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def log_demo(running_task):
    request, name = running_task.request, running_task.name + running_task.request.argsrepr
    if hasattr(request, "stamps"):
        stamps = request.stamps or {}
        stamped_headers = request.stamped_headers or []

    if stamps and stamped_headers:
        logger.critical(f"Found {name}.stamps: {stamps}")
        logger.critical(f"Found {name}.stamped_headers: {stamped_headers}")
    else:
        logger.critical(f"Running {name} without stamps")

    links = request.callbacks or []
    for link in links:
        link = maybe_signature(link)
        logger.critical(f"Found {name}.link: {link}")
        stamped_headers = link.options.get("stamped_headers", [])
        stamps = {stamp: link.options[stamp] for stamp in stamped_headers}

        if stamps and stamped_headers:
            logger.critical(f"Found {name}.link stamps: {stamps}")
            logger.critical(f"Found {name}.link stamped_headers: {stamped_headers}")
        else:
            logger.critical(f"Running {name}.link without stamps")


class StampOnReplace(Task):
    """Custom task for stamping on replace"""

    def on_replace(self, sig: Signature):
        logger.warning(f"StampOnReplace: {sig}.stamp(FullVisitor())")
        sig.stamp(FullVisitor())
        logger.warning(f"StampOnReplace: {sig}.stamp(MyStampingVisitor())")
        sig.stamp(MyStampingVisitor())
        return super().on_replace(sig)


class MonitoredTask(Task):
    def on_replace(self, sig: Signature):
        logger.warning(f"MonitoredTask: {sig}.stamp(MonitoringIdStampingVisitor())")
        sig.stamp(MonitoringIdStampingVisitor(), append_stamps=False)
        return super().on_replace(sig)


@app.task(bind=True)
def identity_task(self, x):
    """Identity function"""
    log_demo(self)
    return x


@app.task(bind=True, base=MonitoredTask)
def replaced_identity(self: MonitoredTask, x):
    log_demo(self)
    logger.warning("Stamping identity_task with MonitoringIdStampingVisitor() before replace")
    replaced_task = identity_task.s(x)
    # These stamps should be overridden by the stamps from MonitoredTask.on_replace()
    replaced_task.stamp(MonitoringIdStampingVisitor())
    return self.replace(replaced_task)


@app.task(bind=True, base=StampOnReplace)
def identity(self: Task, x):
    log_demo(self)
    return self.replace(replaced_identity.s(x))


@app.task
def mul(x: int, y: int) -> int:
    """Multiply two numbers"""
    return x * y


@app.task
def xsum(numbers: list) -> int:
    """Sum a list of numbers"""
    return sum(numbers)


@app.task
def waitfor(seconds: int) -> None:
    """Wait for "seconds" seconds, ticking every second."""
    print(f"Waiting for {seconds} seconds...")
    for i in range(seconds):
        sleep(1)
        print(f"{i+1} seconds passed")


@app.task(bind=True, base=StampOnReplace)
def wait_for_revoke(self: StampOnReplace, seconds: int) -> None:
    """Replace this task with a new task that waits for "seconds" seconds."""
    self.replace(waitfor.s(seconds))
