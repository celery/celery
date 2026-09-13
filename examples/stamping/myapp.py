"""myapp.py

This is a simple example of how to use the stamping feature.
It uses a custom stamping visitor to stamp a workflow with a unique
monitoring id stamp (per task), and a different visitor to stamp the last
task in the workflow. The last task is stamped with a consistent stamp, which
is used to revoke the task by its stamped header using two different approaches:
1. Run the workflow, then revoke the last task by its stamped header.
2. Revoke the last task by its stamped header before running the workflow.

Usage::

   # The worker service reacts to messages by executing tasks.
   (window1)$ celery -A myapp worker -l INFO

   # The shell service is used to run the example.
    (window2)$ celery -A myapp shell

   # Use (copy) the content of the examples modules to run the workflow via the
   # shell service.

   # Use one of demo runs via the shell service:
   # 1) run_then_revoke(): Run the workflow and revoke the last task
   #    by its stamped header during its run.
   # 2) revoke_then_run(): Revoke the last task by its stamped header
   #    before its run, then run the workflow.
   # 3) Any of the examples in examples.py
   #
   # See worker logs for output per defined in task_received_handler().
"""
import json

# Import tasks in worker context
import tasks  # noqa
from config import app

from celery.signals import task_received


@task_received.connect
def task_received_handler(sender=None, request=None, signal=None, **kwargs):
    print(f"In {signal.name} for: {repr(request)}")
    if hasattr(request, "stamped_headers") and request.stamped_headers:
        print(f"Found stamps: {request.stamped_headers}")
        print(json.dumps(request.stamps, indent=4, sort_keys=True))
    else:
        print("No stamps found")


if __name__ == "__main__":
    app.start()
