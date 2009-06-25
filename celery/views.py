"""celery.views"""
from django.http import HttpResponse, Http404
from celery.task import tasks, is_done, apply_async
from celery.result import AsyncResult

try:
    import simplejson as json
except ImportError:
    from django.utils import simplejson as json


def apply(request, task_name, *args):
    """View applying a task.

    Example:
        http://e.com/celery/apply/task_name/arg1/arg2//?kwarg1=a&kwarg2=b

    **NOTE** Use with caution, preferably not make this publicly accessible
    without ensuring your code is safe!

    """
    kwargs = request.method == "POST" and \
            request.POST.copy() or request.GET.copy()
    kwargs = [(key.encode("utf-8"), value)
                    for key, value in kwargs.items()]
    if task_name not in tasks:
        raise Http404("apply: no such task")

    task = tasks[task_name]
    result = apply_async(task, args=args, kwargs=kwargs)
    return json.dumps({"ok": "true", "task_id": result.task_id})


def is_task_done(request, task_id):
    """Returns task execute status in JSON format."""
    response_data = {"task": {"id": task_id, "executed": is_done(task_id)}}
    return HttpResponse(json.dumps(response_data), mimetype="application/json")


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
    return HttpResponse(json.dumps({"task": response_data}),
            mimetype="application/json")
