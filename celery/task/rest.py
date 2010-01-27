from celery.task.http import (InvalidResponseError, RemoteExecuteError,
                              UnknownStatusError)
from celery.task.http import HttpDispatch as RESTProxy
from celery.task.http import HttpDispatchTask as RESTProxyTask
from celery.task.http import http_task_response as task_response
from celery.task.http import URL, Task

import warnings
warnings.warn(DeprecationWarning(
"""celery.task.rest has been deprecated and is scheduled for removal in
v1.2. Please use celery.task.http instead.

The following objects has been renamed:

    celery.task.rest.RESTProxy -> celery.task.http.HttpDispatch
    celery.task.rest.RESTProxyTask -> celery.task.http.HttpDispatchTask
    celery.task.rest.task_response -> celery.task.http.http_task_response

Other objects have the same name, just moved to the celery.task.http module.

"""))
