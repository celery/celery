# Test config for t/integration/test_worker.py

broker_url = 'amqp://guest:guest@foobar:1234//'

# Fail fast for test_run_worker
broker_connection_retry_on_startup = False
broker_connection_retry = False
broker_connection_timeout = 0

worker_log_color = False

worker_redirect_stdouts = False
