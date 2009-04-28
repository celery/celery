"""Distributed Task Queue for Django"""
VERSION = (0, 1, 5)
__version__ = ".".join(map(str, VERSION))
__author__ = "Ask Solem"
__contact__ = "askh@opera.com"
__homepage__ = "http://github.com/ask/celery/"
__docformat__ = "restructuredtext"

__all__ = ["bin", "conf", "discovery", "log", "managers",
           "messaging", "models", "platform", "process",
           "registry", "task", "urls", "views", "worker"]
