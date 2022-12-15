from time import sleep

from config import app

from celery import Task
from examples.stamping.visitors import MyStampingVisitor


class MyTask(Task):
    """Custom task for stamping on replace"""

    def on_replace(self, sig):
        sig.stamp(MyStampingVisitor())
        return super().on_replace(sig)


@app.task
def identity(x):
    """Identity function"""
    return x


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
    print(f'Waiting for {seconds} seconds...')
    for i in range(seconds):
        sleep(1)
        print(f'{i+1} seconds passed')


@app.task(bind=True, base=MyTask)
def wait_for_revoke(self: MyTask, seconds: int) -> None:
    """Replace this task with a new task that waits for "seconds" seconds."""
    # This will stamp waitfor with MyStampingVisitor
    self.replace(waitfor.s(seconds))
