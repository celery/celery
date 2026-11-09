"""Drive a chord's header tasks through a result backend, as workers do."""
from celery import states, uuid
from celery.app.task import Context


class ChordParts:
    """A chord whose body and errback record what they were called with.

    Header tasks return through ``mark_as_done``, the way a worker reports
    them, so a test can return one several times, as the broker can deliver
    a task again. The body runs eagerly when the chord sends it.
    """

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

    def request(self, index):
        return Context(id=self.headers[index].id, group=self.group_id,
                       group_index=index, chord=self.body)

    def returns(self, index, state=states.SUCCESS):
        """Report header task ``index`` as finished, with its index as result."""
        request = self.request(index)
        if state == states.SUCCESS:
            self.backend.mark_as_done(request.id, index, request=request)
        else:
            self.backend.mark_as_failure(
                request.id, KeyError(index), request=request)

    def retries(self, index):
        """Report header task ``index`` as retrying, with the same task id."""
        request = self.request(index)
        self.backend.mark_as_retry(request.id, KeyError(index), request=request)

    def expire_group(self):
        """Remove the saved group, as when the chord outlived its expiry."""
        self.backend.delete(self.backend.get_key_for_group(self.group_id))


def random_chord_events(rng):
    """Return a chord size and a random order of its header tasks' reports.

    Each header task returns one to three times, as the broker can deliver
    it again, after zero or more retries. Some chords lose their saved
    group, as when they outlive their expiry, at a random point.
    """
    size = rng.randint(1, 4)
    pending = [
        [('retries', i)] * rng.randint(0, 2) + [('returns', i)] * rng.randint(1, 3)
        for i in range(size)
    ]
    events = []
    while any(pending):
        events.append(rng.choice([p for p in pending if p]).pop(0))
    if rng.random() < 0.2:
        events.insert(rng.randrange(len(events) + 1), ('expire_group', None))
    return size, events


def check_random_chords(app, backend, seeds):
    """Check random chords against a model of counting each part once.

    The body must run once, with every header result, unless the group was
    lost before the last header task's first return: then each header task
    that returns for the first time after that fails the chord, and the
    body never runs.
    """
    import random
    for seed in seeds:
        size, events = random_chord_events(random.Random(seed))
        chord = ChordParts(app, backend, size)
        counted, expired, errors = set(), False, 0
        for event, index in events:
            if event == 'expire_group':
                expired = expired or len(counted) < size
                chord.expire_group()
                continue
            getattr(chord, event)(index)
            if event == 'returns' and index not in counted:
                counted.add(index)
                errors += expired
        expected_bodies = [] if expired else [list(range(size))]
        context = f'seed={seed} size={size} events={events}'
        assert chord.bodies == expected_bodies, context
        assert len(chord.errors) == errors, context
