"""celery.views"""
from django.http import HttpResponse
from celery.task import is_done, delay_task
from celery.result import AsyncResult
import simplejson


def is_task_done(request, task_id):
    """Returns task execute status in JSON format."""
    response_data = {"task": {"id": task_id, "executed": is_done(task_id)}}
    return HttpResponse(simplejson.dumps(response_data))


def task_status(request, task_id):
    """Returns task status and result in JSON format."""
    async_result = AsyncResult(task_id)
    response_data = {"task": {
                        "id": task_id,
                        "status": async_result.status,
                        "result": async_result.result,
    }}
    return HttpResponse(simplejson.dumps(response_data))
