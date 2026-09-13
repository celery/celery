import pytest

from celery import states


class test_state_precedence:

    @pytest.mark.parametrize('r,l', [
        (states.SUCCESS, states.PENDING),
        (states.FAILURE, states.RECEIVED),
        (states.REVOKED, states.STARTED),
        (states.SUCCESS, 'CRASHED'),
        (states.FAILURE, 'CRASHED'),
    ])
    def test_gt(self, r, l):
        assert states.state(r) > states.state(l)

    @pytest.mark.parametrize('r,l', [
        ('CRASHED', states.REVOKED),
    ])
    def test_gte(self, r, l):
        assert states.state(r) >= states.state(l)

    @pytest.mark.parametrize('r,l', [
        (states.PENDING, states.SUCCESS),
        (states.RECEIVED, states.FAILURE),
        (states.STARTED, states.REVOKED),
        ('CRASHED', states.SUCCESS),
        ('CRASHED', states.FAILURE),
        (states.REVOKED, 'CRASHED'),
    ])
    def test_lt(self, r, l):
        assert states.state(r) < states.state(l)

    @pytest.mark.parametrize('r,l', [
        (states.REVOKED, 'CRASHED'),
    ])
    def test_lte(self, r, l):
        assert states.state(r) <= states.state(l)
