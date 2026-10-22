import subprocess
import sys
import textwrap

import pytest


@pytest.mark.parametrize('multi,template,expected', [
    (False, '%%n-%n.log', '%n-worker.log'),
    (False, '%%x.log', '%x.log'),
    (True, '%%%%n-%n.log', '%n-worker.log'),
    (True, '50%%%%d.log', '50%d.log'),
    (True, '%%%%x.log', '%x.log'),
    (True, '%%%%i-%i.log', '%i-0.log'),
])
def test_escaped_logfile_reaches_logging(tmp_path, multi, template, expected):
    # Isolate Celery's global logging state while exercising real file handlers.
    script = textwrap.dedent("""
        import logging
        import sys
        from pathlib import Path
        from celery import Celery
        from celery.apps.multi import Node

        directory, multi, template = sys.argv[1:]
        directory = Path(directory)
        hostname = 'worker@example.com'
        logfile = str(directory / template)
        if multi == 'True':
            node = Node(hostname, options={
                '--logfile': logfile,
                '--pidfile': str(directory / 'worker.pid'),
            })
            logfile = next(arg.partition('=')[2] for arg in node.argv
                           if arg.startswith('--logfile='))

        app = Celery('percent-formatting')
        app.log.setup_logging_subsystem(
            loglevel='INFO', logfile=logfile, hostname=hostname, colorize=False,
        )
        logging.getLogger('percent-formatting').info('escaped logfile reached')
        logging.shutdown()
        app.close()
    """)
    subprocess.run(
        [sys.executable, '-c', script, str(tmp_path), str(multi), template],
        check=True, capture_output=True, text=True, timeout=30,
    )
    assert 'escaped logfile reached' in (tmp_path / expected).read_text()
