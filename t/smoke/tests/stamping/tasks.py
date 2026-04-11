from time import sleep

from celery import shared_task
from t.integration.tasks import LEGACY_TASKS_DISABLED


@shared_task
def waitfor(seconds: int) -> None:
    print(f"Waiting for {seconds} seconds...")
    for i in range(seconds):
        sleep(1)
        print(f"{i+1} seconds passed")
    print("Done waiting")


if LEGACY_TASKS_DISABLED:
    from t.integration.tasks import StampedTaskOnReplace, StampOnReplace

    @shared_task(bind=True, base=StampedTaskOnReplace)
    def wait_for_revoke(self: StampOnReplace, seconds: int, waitfor_worker_queue) -> None:
        print(f"Replacing {self.request.id} with waitfor({seconds})")
        self.replace(waitfor.s(seconds).set(queue=waitfor_worker_queue))
