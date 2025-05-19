import io
from datetime import datetime

from celery.events import dumper


def test_humanize_type():
    assert dumper.humanize_type('worker-online') == 'started'
    assert dumper.humanize_type('worker-offline') == 'shutdown'
    assert dumper.humanize_type('worker-heartbeat') == 'heartbeat'


def test_dumper_say():
    buf = io.StringIO()
    d = dumper.Dumper(out=buf)
    d.say('hello world')
    assert 'hello world' in buf.getvalue()


def test_format_task_event_output():
    buf = io.StringIO()
    d = dumper.Dumper(out=buf)
    d.format_task_event(
        hostname='worker1',
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        type='task-succeeded',
        task='mytask(123) args=(1,) kwargs={}',
        event={'result': 'ok', 'foo': 'bar'}
    )
    output = buf.getvalue()
    assert 'worker1 [2024-01-01 12:00:00]' in output
    assert 'task succeeded' in output
    assert 'mytask(123) args=(1,) kwargs={}' in output
    assert 'result=ok' in output
    assert 'foo=bar' in output


def test_on_event_task_received():
    buf = io.StringIO()
    d = dumper.Dumper(out=buf)
    event = {
        'timestamp': datetime(2024, 1, 1, 12, 0, 0).timestamp(),
        'type': 'task-received',
        'hostname': 'worker1',
        'uuid': 'abc',
        'name': 'mytask',
        'args': '(1,)',
        'kwargs': '{}',
    }
    d.on_event(event.copy())
    output = buf.getvalue()
    assert 'worker1 [2024-01-01 12:00:00]' in output
    assert 'task received' in output
    assert 'mytask(abc) args=(1,) kwargs={}' in output


def test_on_event_non_task():
    buf = io.StringIO()
    d = dumper.Dumper(out=buf)
    event = {
        'timestamp': datetime(2024, 1, 1, 12, 0, 0).timestamp(),
        'type': 'worker-online',
        'hostname': 'worker1',
        'foo': 'bar',
    }
    d.on_event(event.copy())
    output = buf.getvalue()
    assert 'worker1 [2024-01-01 12:00:00]' in output
    assert 'started' in output
    assert 'foo=bar' in output
