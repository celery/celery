from django.http import Http404, HttpResponse
from celery.task import is_done

JSON_TASK_STATUS = """{"task": {"id": "%s", "executed": %s}}"""


def is_task_done(request, task_id):
    return HttpResponse(JSON_TASK_STATUS % (
        task_id, is_done(task_id) and "true" or "false"))
