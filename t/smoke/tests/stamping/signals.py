import json

from celery.signals import task_received


@task_received.connect
def task_received_handler(request, **kwargs):
    stamps = request.request_dict.get("stamps")
    stamped_headers = request.request_dict.get("stamped_headers")
    stamps_dump = json.dumps(stamps, indent=4, sort_keys=True) if stamps else stamps
    print(f"stamped_headers = {stamped_headers}")
    print(f"stamps = {stamps_dump}")
