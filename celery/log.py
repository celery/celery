# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import current_app
from .local import Proxy
from .utils.log import LOG_LEVELS, LoggingProxy  # noqa

get_default_logger = Proxy(lambda: current_app.log.get_default_logger)
setup_logger = Proxy(lambda: current_app.log.setup_logger)
setup_task_logger = Proxy(lambda: current_app.log.setup_task_logger)
get_task_logger = Proxy(lambda: current_app.log.get_task_logger)
setup_logging_subsystem = Proxy(
        lambda: current_app.log.setup_logging_subsystem)
redirect_stdouts_to_logger = Proxy(
        lambda: current_app.log.redirect_stdouts_to_logger)
