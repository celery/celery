import json

from celery.signals import task_received


@task_received.connect
def task_received_handler(request, **kwargs):
    if not hasattr(request, "stamps") or not hasattr(request, "stamped_headers"):
        print("No stamps found")
        return

    stamps_dump = json.dumps(request.stamps, indent=4, sort_keys=True)
    print(f"{request.stamped_headers = }")  # noqa
    print(f"request.stamps = {stamps_dump}")
