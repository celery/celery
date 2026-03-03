from celery import Celery
from celery.worker.control import control_command, inspect_command


@control_command(
    args=[('a', int), ('b', int)],
    signature='a b',
)
def custom_control_cmd(state, a, b):
    """Ask the workers to reply with a and b."""
    return {'ok': f'Received {a} and {b}'}


@inspect_command(
    args=[('x', int)],
    signature='x',
)
def custom_inspect_cmd(state, x):
    """Ask the workers to reply with x."""
    return {'ok': f'Received {x}'}


app = Celery(set_as_current=False)
app.config_from_object('t.integration.test_worker_config')
