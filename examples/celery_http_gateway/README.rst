==============================
 Example Celery->HTTP Gateway
==============================

This is an example service exposing the ability to apply tasks and query
statuses/results over HTTP.

Some familiarity with Django is recommended.

`settings.py` contains the celery settings, you probably want to configure
at least the broker related settings.

To run the service you have to run the following commands::

    $ python manage.py syncdb # (if running the database backend)

    $ python manage.py runserver


The service is now running at http://localhost:8000


You can apply tasks, with the `/apply/<task_name>` URL::

    $ curl http://localhost:8000/apply/celery.ping/
    {"ok": "true", "task_id": "e3a95109-afcd-4e54-a341-16c18fddf64b"}

Then you can use the resulting task-id to get the return value::

    $ curl http://localhost:8000/e3a95109-afcd-4e54-a341-16c18fddf64b/status/
    {"task": {"status": "SUCCESS", "result": "pong", "id": "e3a95109-afcd-4e54-a341-16c18fddf64b"}}


If you don't want to expose all tasks there are a few possible
approaches. For instance you can extend the `apply` view to only
accept a whitelist. Another possibility is to just make views for every task you want to
expose. We made on such view for ping in `views.ping`::

    $ curl http://localhost:8000/ping/
    {"ok": "true", "task_id": "383c902c-ba07-436b-b0f3-ea09cc22107c"}
