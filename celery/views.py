from django.http import Http404, HttpResponse
from celery.task import is_done
import simplejson


def is_task_done(request, task_id):
    response_data = {"task": {"id": task_id, "executed": is_done(task_id)}}
    return HttpResponse(simplejson.dumps(response_data))
