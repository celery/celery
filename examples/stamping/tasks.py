from time import sleep

from config import app
from visitors import FullVisitor, MonitoringIdStampingVisitor, MyStampingVisitor

from celery import Task
from celery.canvas import Signature


class MyTask(Task):
    """Custom task for stamping on replace"""

    def on_replace(self, sig: Signature):
        sig.stamp(MyStampingVisitor())
        return super().on_replace(sig)


@app.task
def identity_task(x):
    """Identity function"""
    # When used from identity(), this task will be stamped with:
    # - FullVisitor: Stamps per canvas primitive:
    # e.g: on_signature: {
    #        "on_signature": "FullVisitor.on_signature()",
    #    }
    # - MyStampingVisitor: {"mystamp": "I am a stamp!"}
    # - MonitoringIdStampingVisitor: {"monitoring_id": str(uuid4())}
    return x


@app.task(bind=True)
def replaced_identity(self: Task, x):
    # Adds stamps to identity_task from: MonitoringIdStampingVisitor
    return self.replace(identity_task.s(x), visitor=MonitoringIdStampingVisitor())


@app.task(bind=True, base=MyTask)
def identity(self: Task, x):
    # Adds stamps to replaced_identity from: FullVisitor and MyStampingVisitor
    return self.replace(replaced_identity.s(x), visitor=FullVisitor())


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


@app.task(bind=True, base=MyTask)
def wait_for_revoke(self: MyTask, seconds: int) -> None:
    """Replace this task with a new task that waits for "seconds" seconds."""
    # This will stamp waitfor with MyStampingVisitor
    self.replace(waitfor.s(seconds))
