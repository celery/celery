from unittest.mock import Mock, patch

from celery.apps.beat import Beat


class test_Beat_remote_control:
    """The remote-control knobs must survive the trip to the scheduler.

    ``Beat.__init__`` swallows unknown keyword arguments, so a flag that
    isn't threaded through explicitly is silently dropped rather than
    raising -- which is exactly how it goes unnoticed.
    """

    def _service_kwargs(self, app, **kwargs):
        captured = {}

        class CapturingService:
            def __init__(self, **service_kwargs):
                captured.update(service_kwargs)
                self.remote_control = service_kwargs['remote_control']
                self.hostname = service_kwargs['hostname']

            def start(self):
                pass

        beat = Beat(app=app, quiet=True,
                    scheduler_cls='celery.beat:Scheduler', **kwargs)
        beat.Service = CapturingService
        with patch.object(Beat, 'install_sync_handler'), \
                patch.object(Beat, 'setup_logging'):
            beat.start_scheduler()
        return captured

    def test_flags_reach_the_service(self, app):
        captured = self._service_kwargs(
            app, remote_control=True, hostname='probe@example.com')
        assert captured['remote_control'] is True
        assert captured['hostname'] == 'probe@example.com'

    def test_defaults_are_left_for_the_service_to_resolve(self, app):
        # None, not False: the Service falls back to the setting, so the
        # CLI must be able to say "no opinion".
        captured = self._service_kwargs(app)
        assert captured['remote_control'] is None
        assert captured['hostname'] is None

    def test_banner_omits_remote_control_when_disabled(self, app):
        beat = Beat(app=app, quiet=True)
        service = Mock(name='service')
        service.remote_control = False
        assert beat._remote_control_info(service) == ''

    def test_banner_names_the_node_when_enabled(self, app):
        beat = Beat(app=app, quiet=True)
        service = Mock(name='service')
        service.remote_control = True
        service.hostname = 'probe@example.com'
        assert beat._remote_control_info(service) == (
            '    . remote control -> probe@example.com')
