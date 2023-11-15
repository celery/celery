import json

from celery.signals import task_received


@task_received.connect
def task_received_handler(request, **kwargs):
    stamps = request.request_dict.get("stamps")
    stamped_headers = request.request_dict.get("stamped_headers")

    if not stamps or not stamped_headers:
        print("No stamps found")
        return

    stamps_dump = json.dumps(stamps, indent=4, sort_keys=True)
    print(f"stamped_headers = {stamped_headers}")
    print(f"stamps = {stamps_dump}")
