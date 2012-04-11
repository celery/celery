from __future__ import absolute_import

from celery import current_app
from celery.local import Proxy


broadcast = Proxy(lambda: current_app.control.broadcast)
rate_limit = Proxy(lambda: current_app.control.rate_limit)
time_limit = Proxy(lambda: current_app.control.time_limit)
ping = Proxy(lambda: current_app.control.ping)
revoke = Proxy(lambda: current_app.control.revoke)
discard_all = Proxy(lambda: current_app.control.discard_all)
inspect = Proxy(lambda: current_app.control.inspect)
