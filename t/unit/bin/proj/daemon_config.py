# Test config for t/unit/bin/test_deamonization.py

beat_pidfile = "/tmp/beat.test.pid"
beat_logfile = "/tmp/beat.test.log"
beat_uid = 42
beat_gid = 4242
beat_umask = 0o777
beat_executable = "/beat/bin/python"

events_pidfile = "/tmp/events.test.pid"
events_logfile = "/tmp/events.test.log"
events_uid = 42
events_gid = 4242
events_umask = 0o777
events_executable = "/events/bin/python"

worker_pidfile = "/tmp/worker.test.pid"
worker_logfile = "/tmp/worker.test.log"
worker_uid = 42
worker_gid = 4242
worker_umask = 0o777
worker_executable = "/worker/bin/python"
