import socket

from celery.app import app_or_default
from celery.worker.control.registry import Panel

# Loads the built-in remote control commands
__import__("celery.worker.control.builtins")
