from celery.worker.control import registry

Panel = registry.Panel

# Loads the built-in remote control commands
__import__("celery.worker.control.builtins")
