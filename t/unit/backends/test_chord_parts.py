"""Chord header tasks that return more than once, on the incr backends.

A worker reports a header task through ``mark_as_done``, and the broker can
deliver a task again, so a test can report one several times. Each test runs
against the memory and memcached cache backends, DynamoDB (moto) and GCS
(in-memory fakes of its storage and Firestore).
"""
import random
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from kombu.utils.encoding import bytes_to_str
from moto import mock_aws

from celery import uuid
from celery.app.task import Context
from celery.backends.cache import CacheBackend
from t.unit import conftest
from t.unit.backends.test_cache import MockPyMemcacheMixin


class Killed(BaseException):
    """Stands for a SIGKILL: no handler in the backend may catch it."""


class ChordParts:
    """A chord whose body and errback record what they were called with."""

    def __init__(self, app, backend, size=2, task_ids=None):
        # Header results restore with the app's backend, as in a worker.
        app._backend = self.backend = backend
        self.bodies, self.errors = [], []
        bodies, errors = self.bodies, self.errors
        app.conf.task_always_eager = True
        app.conf.task_store_eager_result = True

        @app.task(name=f'chord_parts.body.{uuid()}', shared=False)
        def body(results):
            bodies.append(results)

        @app.task(name=f'chord_parts.errback.{uuid()}', shared=False)
        def errback(request, exc, traceback):
            errors.append(exc)

        self.group_id = uuid()
        task_ids = task_ids or [uuid() for _ in range(size)]
        self.headers = [app.AsyncResult(task_id) for task_id in task_ids]
        self.body = body.s().on_error(errback.s())
        self.body.freeze()
        backend.apply_chord((self.group_id, self.headers), self.body)

    def returns(self, index):
        """Report header task ``index`` as done, with its index as result."""
        request = Context(id=self.headers[index].id, group=self.group_id,
                          group_index=index, chord=self.body)
        self.backend.mark_as_done(request.id, index, request=request)

    def killed(self, index):
        """Report header task ``index``, killed after it stored its result."""
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(self.backend, 'incr', side_effect=Killed))
            # As with SIGKILL, the backend can't clean up after this run.
            for cleanup in ('delete', '_delete_chord_key'):
                if hasattr(self.backend, cleanup):
                    stack.enter_context(patch.object(self.backend, cleanup))
            try:
                self.returns(index)
            except Killed:
                pass

    def together(self, first, second):
        """Report ``second`` while ``first`` is right after its count."""
        incr = self.backend.incr

        def incr_then_second(key):
            value = incr(key)
            with patch.object(self.backend, 'incr', incr):
                self.returns(second)
            return value

        with patch.object(self.backend, 'incr', side_effect=incr_then_second):
            self.returns(first)

    def retries(self, index):
        """Report header task ``index`` as retrying, with the same task id."""
        task_id = self.headers[index].id
        self.backend.mark_as_retry(task_id, KeyError(index))

    def expire_group(self):
        """Remove the saved group, as when the chord outlived its expiry."""
        self.backend.delete(self.backend.get_key_for_group(self.group_id))


class FakeBlob:
    def __init__(self, blobs, key):
        self.blobs, self.key = blobs, key

    def download_as_bytes(self, retry=None):
        from celery.backends.gcs import storage
        if self.key not in self.blobs:
            raise storage.blob.NotFound('no blob')
        return self.blobs[self.key]

    def upload_from_string(self, value, retry=None):
        self.blobs[self.key] = value

    def exists(self):
        return self.key in self.blobs

    def delete(self, retry=None):
        del self.blobs[self.key]


class FakeDocument:
    """A Firestore document, with the operations the GCS backend uses."""

    def __init__(self, docs, key):
        self.docs, self.key = docs, bytes_to_str(key)

    def set(self, data, merge=False, retry=None):
        from celery.backends.gcs import firestore
        doc = self.docs.setdefault(self.key, {}) if merge else {}
        for field, value in data.items():
            if isinstance(value, firestore.Increment):
                value = doc.get(field, 0) + value.value
            doc[field] = value
        self.docs[self.key] = doc
        return SimpleNamespace(transform_results=[
            SimpleNamespace(integer_value=value)])

    def create(self, data, retry=None):
        from google.api_core.exceptions import AlreadyExists
        if self.key in self.docs:
            raise AlreadyExists('document exists')
        self.docs[self.key] = dict(data)

    def get(self):
        return SimpleNamespace(to_dict=lambda: self.docs.get(self.key))

    def delete(self):
        self.docs.pop(self.key, None)


@pytest.fixture(params=['memory', 'pymemcache', 'dynamodb', 'gcs'])
def backend(request, app):
    # A chord that completes too early fails on the join.
    app.conf.result_chord_join_timeout = 0.1
    if request.param == 'memory':
        yield CacheBackend(backend='memory://', app=app)
    elif request.param == 'pymemcache':
        with MockPyMemcacheMixin().mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                yield cache.CacheBackend(
                    backend='memcache://127.0.0.1:11211/', app=app)
    elif request.param == 'dynamodb':
        from celery.backends.dynamodb import DynamoDBBackend
        with mock_aws():
            yield DynamoDBBackend(app=app, url='dynamodb://@us-east-1')
    else:
        from celery.backends.gcs import GCSBackend
        app.conf.gcs_bucket, app.conf.gcs_project = 'bucket', 'project'
        with patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled',
                          return_value=True):
            gcs = GCSBackend(app=app)
        blobs, docs = {}, {}
        gcs._get_blob = lambda key: FakeBlob(blobs, key)
        gcs._firestore_document = lambda key: FakeDocument(docs, key)
        yield gcs


def test_a_header_task_returning_twice_counts_once(app, backend):
    chord = ChordParts(app, backend)
    chord.returns(0)
    chord.returns(0)
    assert chord.bodies == []
    chord.returns(1)
    assert chord.bodies == [[0, 1]]
    assert chord.errors == []


def test_a_copy_after_completion_calls_no_errback(app, backend):
    chord = ChordParts(app, backend)
    chord.returns(0)
    chord.returns(1)
    chord.returns(1)
    chord.returns(0)
    assert chord.bodies == [[0, 1]]
    assert chord.errors == []


def test_a_header_task_killed_before_it_counted_counts_again(app, backend):
    chord = ChordParts(app, backend)
    chord.killed(0)
    chord.returns(1)
    assert chord.bodies == []
    chord.returns(0)
    assert chord.bodies == [[0, 1]]
    assert chord.errors == []


def test_an_expired_group_fails_the_chord_once(app, backend):
    chord = ChordParts(app, backend)
    chord.returns(0)
    chord.expire_group()
    chord.returns(1)
    chord.returns(1)
    assert chord.bodies == []
    assert [str(exc) for exc in chord.errors] == [
        f'GroupResult {chord.group_id} no longer exists']


def test_header_entries_sharing_a_task_id(app, backend):
    # A frozen signature used twice in a header has one task id.
    task_id = uuid()
    chord = ChordParts(app, backend, task_ids=[task_id, task_id])
    chord.returns(0)
    chord.returns(1)
    assert len(chord.bodies) == 1
    assert chord.errors == []


def test_add_chord_key_counts_a_retried_write_as_its_own(backend):
    # A client that lost the reply to its write sends it again.
    key = backend.get_key_for_chord(uuid(), '.done')
    assert backend._add_chord_key(key, 'run 1') is True
    assert backend._add_chord_key(key, 'run 1') is True
    assert backend._add_chord_key(key, 'run 2') is False


@pytest.mark.parametrize('backend', ['memory'], indirect=True)
def test_a_worker_of_an_earlier_version_runs_a_part(app, backend):
    chord = ChordParts(app, backend)
    with patch.object(backend, 'implements_add', False):
        chord.returns(0)
    chord.returns(1)
    assert chord.bodies == [[0, 1]]
    assert chord.errors == []


def random_chord_events(rng):
    """Return a chord size and a random order of its header tasks' reports.

    Each header task returns one to three times, after zero to two retries.
    Some runs are killed before they count, some return while another part
    is right after its count, and some chords lose their saved group.
    """
    size = rng.randint(1, 4)
    pending = []
    for i in range(size):
        runs = [('returns', i)] * rng.randint(1, 3)
        if rng.random() < 0.3:
            runs.insert(rng.randrange(len(runs) + 1), ('killed', i))
        pending.append([('retries', i)] * rng.randint(0, 2) + runs)
    events = []
    while any(pending):
        event = rng.choice([p for p in pending if p]).pop(0)
        others = [p for p in pending if p and p[0][0] == 'returns']
        if event[0] == 'returns' and others and rng.random() < 0.2:
            event = ('together', (event[1], rng.choice(others).pop(0)[1]))
        events.append(event)
    if rng.random() < 0.2:
        events.insert(rng.randrange(len(events) + 1), ('expire_group', None))
    return size, events


def test_random_chords(app, backend):
    """Check random chords against a model of the chord's bookkeeping.

    Every run stores its result, then counts. The chord completes once, at
    the first count that reaches its size once every result is stored. The
    first run that finds the group lost before that fails the chord. GCS
    counts before it reads the group, so a run killed before it counts
    doesn't see a lost group there.
    """
    gcs = type(backend).__name__ == 'GCSBackend'
    for seed in range(300 if isinstance(backend, CacheBackend) else 50):
        size, events = random_chord_events(random.Random(seed))
        chord = ChordParts(app, backend, size)
        stored, count = set(), 0
        completed = expired = done = False
        errors = 0

        def run(index, killed=False):
            # Return whether the run counted.
            nonlocal count, done, errors
            if done:
                return False
            if expired and not (gcs and killed):
                done = True
                errors += 1
                return False
            stored.add(index)
            count += not killed
            return not killed

        def check():
            nonlocal completed, done
            if not done and count >= size and len(stored) == size:
                completed = done = True

        for event, arg in events:
            if event == 'expire_group':
                expired = expired or not done
                chord.expire_group()
                continue
            getattr(chord, event)(*(arg if event == 'together' else (arg,)))
            if event == 'returns' and run(arg):
                check()
            elif event == 'killed':
                run(arg, killed=True)
            elif event == 'together' and run(arg[0]):
                if run(arg[1]):
                    check()
                check()
        context = f'seed={seed} size={size} events={events}'
        assert chord.bodies == ([list(range(size))] if completed else []), context
        assert len(chord.errors) == errors, context
