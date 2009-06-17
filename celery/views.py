"""celery.views"""
from django.http import HttpResponse
from celery.task import is_done, delay_task
from celery.result import AsyncResult
from carrot.serialization import serialize as JSON_dump


def is_task_done(request, task_id):
    """Returns task execute status in JSON format."""
    response_data = {"task": {"id": task_id, "executed": is_done(task_id)}}
    return HttpResponse(JSON_dump(response_data), mimetype="application/json")


def task_status(request, task_id):
    """Returns task status and result in JSON format."""
    async_result = AsyncResult(task_id)
    status = async_result.status
    if status == "FAILURE":
        response_data = {
            "id": task_id,
            "status": status,
            "result": async_result.result.args[0],
        }
    else:
        response_data = {
            "id": task_id,
            "status": status,
            "result": async_result.result,
        }
    return HttpResponse(JSON_dump({"task": response_data}),
            mimetype="application/json")
