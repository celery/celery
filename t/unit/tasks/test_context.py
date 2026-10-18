from celery.app.task import Context


# Retrieve the values of all context attributes as a
# dictionary in an implementation-agnostic manner.
def get_context_as_dict(ctx, getter=getattr):
    defaults = {}
    for attr_name in dir(ctx):
        if attr_name.startswith('_'):
            continue   # Ignore pseudo-private attributes
        attr = getter(ctx, attr_name)
        if callable(attr):
            continue   # Ignore methods and other non-trivial types
        defaults[attr_name] = attr
    return defaults


default_context = get_context_as_dict(Context())


class test_Context:

    def test_default_context(self):
        # A bit of a tautological test, since it uses the same
        # initializer as the default_context constructor.
        defaults = dict(default_context, children=[])
        assert get_context_as_dict(Context()) == defaults

    def test_updated_context(self):
        expected = dict(default_context)
        changes = {'id': 'unique id', 'args': ['some', 1], 'wibble': 'wobble'}
        ctx = Context()
        expected.update(changes)
        ctx.update(changes)
        assert get_context_as_dict(ctx) == expected
        assert get_context_as_dict(Context()) == default_context

    def test_modified_context(self):
        expected = dict(default_context)
        ctx = Context()
        expected['id'] = 'unique id'
        expected['args'] = ['some', 1]
        ctx.id = 'unique id'
        ctx.args = ['some', 1]
        assert get_context_as_dict(ctx) == expected
        assert get_context_as_dict(Context()) == default_context

    def test_cleared_context(self):
        changes = {'id': 'unique id', 'args': ['some', 1], 'wibble': 'wobble'}
        ctx = Context()
        ctx.update(changes)
        ctx.clear()
        defaults = dict(default_context, children=[])
        assert get_context_as_dict(ctx) == defaults
        assert get_context_as_dict(Context()) == defaults

    def test_context_get(self):
        expected = dict(default_context)
        changes = {'id': 'unique id', 'args': ['some', 1], 'wibble': 'wobble'}
        ctx = Context()
        expected.update(changes)
        ctx.update(changes)
        ctx_dict = get_context_as_dict(ctx, getter=Context.get)
        assert ctx_dict == expected
        assert get_context_as_dict(Context()) == default_context

    def test_extract_headers(self):
        # Should extract custom headers from the request dict
        request = {
            'task': 'test.test_task',
            'id': 'e16eeaee-1172-49bb-9098-5437a509ffd9',
            'custom-header': 'custom-value',
        }
        ctx = Context(request)
        assert ctx.headers == {'custom-header': 'custom-value'}

    def test_dont_override_headers(self):
        # Should not override headers if defined in the request
        request = {
            'task': 'test.test_task',
            'id': 'e16eeaee-1172-49bb-9098-5437a509ffd9',
            'headers': {'custom-header': 'custom-value'},
            'custom-header-2': 'custom-value-2',
        }
        ctx = Context(request)
        assert ctx.headers == {'custom-header': 'custom-value'}

    def test_as_execution_options_ignores_undeclared_stamps(self):
        ctx = Context(
            stamped_headers=['team'],
            stamps={'team': 'payments', 'extra': 'unexpected'},
        )

        options = ctx.as_execution_options()

        assert options['stamped_headers'] == ['team']
        assert options['team'] == 'payments'
        assert 'extra' not in options

    def test_as_execution_options_undeclared_stamp_cannot_override_retries(self):
        ctx = Context(
            retries=2,
            stamped_headers=['team'],
            stamps={'team': 'payments', 'retries': 99},
        )

        options = ctx.as_execution_options()

        assert options['retries'] == 2

    def test_as_execution_options_empty_stamped_headers_ignores_stamps(self):
        ctx = Context(stamped_headers=[], stamps={'extra': 'unexpected'})

        options = ctx.as_execution_options()

        assert options['stamped_headers'] == []
        assert 'extra' not in options

    def test_as_execution_options_stamps_in_task_message(self, app):
        ctx = Context(
            id='task-id',
            retries=2,
            stamped_headers=['team', 'trace_id'],
            stamps={
                'team': 'payments',
                'trace_id': ['trace-1', 'trace-2'],
                'retries': 99,
                'extra': 'unexpected',
            },
        )

        message = app.amqp.as_task_v2(name='test.task', **ctx.as_execution_options())

        assert message.headers['stamped_headers'] == ['team', 'trace_id']
        assert message.headers['stamps'] == {
            'team': 'payments',
            'trace_id': ['trace-1', 'trace-2'],
        }
        assert message.headers['retries'] == 2

    # ------------------------------------------------------------------
    # Context.update() – timelimit detection with non-Mapping iterables
    # ------------------------------------------------------------------

    def test_update_timelimit_via_dict(self):
        """update({'timelimit': [30, 20]}) unpacks time_limit and soft_time_limit."""
        ctx = Context()
        ctx.update({'timelimit': [30, 20]})
        assert ctx.time_limit == 30
        assert ctx.soft_time_limit == 20

    def test_update_timelimit_via_list_of_pairs(self):
        """update([('timelimit', [30, 20])]) must also unpack correctly.

        dict.update() accepts an iterable of (key, value) pairs, so
        Context.update() must handle that form too — previously provided_timelimit
        stayed False because 'timelimit' in list_of_pairs checks for membership
        among the tuples, not among the keys.
        """
        ctx = Context()
        ctx.update([('timelimit', [30, 20])])
        assert ctx.time_limit == 30
        assert ctx.soft_time_limit == 20

    def test_update_timelimit_via_dict_items(self):
        """update(some_dict.items()) is equivalent to update(some_dict) and must unpack."""
        ctx = Context()
        source = {'timelimit': [60, 45]}
        ctx.update(source.items())
        assert ctx.time_limit == 60
        assert ctx.soft_time_limit == 45

    def test_update_timelimit_via_kwarg(self):
        """update(timelimit=[30, 20]) via keyword argument still unpacks."""
        ctx = Context()
        ctx.update(timelimit=[30, 20])
        assert ctx.time_limit == 30
        assert ctx.soft_time_limit == 20

    def test_update_timelimit_none_clears_limits(self):
        """Explicitly passing timelimit=None clears previously set limits."""
        ctx = Context()
        ctx.update({'timelimit': [30, 20]})
        ctx.update([('timelimit', None)])
        assert ctx.time_limit is None
        assert ctx.soft_time_limit is None

    def test_update_without_timelimit_does_not_touch_limits(self):
        """An update that does not contain 'timelimit' must not alter time_limit."""
        ctx = Context()
        ctx.update({'timelimit': [30, 20]})
        ctx.update([('id', 'abc')])
        # time_limit / soft_time_limit must be preserved
        assert ctx.time_limit == 30
        assert ctx.soft_time_limit == 20
