from celery import Celery
from celery.canvas import chain, group

app = Celery(
    "myapp",
    broker="amqp://localhost:5673",
)

app.conf.update(
        accept_content=["json"],
        task_serializer="json",
        result_serializer="json",
        event_serializer="json",
        # broker_url=get_broker_url(ssl=False),
        broker_use_ssl=False,
        broker_transport_options={"confirm_publish": True},
        broker_login_method="PLAIN",
        # result_backend="dbcache+bas://dlj2backendsvc-1.4",
        worker_prefetch_multiplier=1,
        worker_concurrency=16,
        task_acks_late=False,
        task_reject_on_worker_lost=True,
        task_result_expires=60 * 60 * 24,  # 24 hours
        task_publish_retry_policy={
            # "errback": kombu_error_handler,
            "max_retries": 20,
            "interval_start": 1,
            "interval_step": 2,
            "interval_max": 30,
            # "retry_errors": (MessageNacked,),
        },
        task_default_delivery_mode=1,
        worker_enable_remote_control=False,
        worker_cancel_long_running_tasks_on_connection_loss=True,
    )


@app.task
def identity(x):
    return x


@app.task
def fail():
    raise Exception("fail")


chain_sig = chain(
    identity.si("start"),
    group(
        identity.si("a"),
        fail.si(),
    ),
    identity.si("break"),
    identity.si("end"),
)

# chain_sig = identity.si("break") | identity.si("end")
# chain_sig.freeze()
# result = chain_sig.apply_async()

if __name__ == "__main__":
    app.start()
