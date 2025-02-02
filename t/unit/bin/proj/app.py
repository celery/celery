from celery import Celery

app = Celery(set_as_current=False)
app.config_from_object("t.integration.test_worker_config")
