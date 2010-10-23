from celery.worker.control.registry import Panel

# Loads the built-in remote control commands
__import__("celery.worker.control.builtins")
