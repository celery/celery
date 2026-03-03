from unittest.mock import MagicMock, Mock

from click import Option

from celery import Celery

# This module defines a mocked Celery application to replicate
# the behavior of Pyramid-Celery's configuration by preload options.
# Preload options should propagate to commands like shell and purge etc.
#
# The Pyramid-Celery project https://github.com/sontek/pyramid_celery
# assumes that you want to configure Celery via an ini settings file.
# The .ini files are the standard configuration file for Pyramid
# applications.
# See https://docs.pylonsproject.org/projects/pyramid/en/latest/quick_tutorial/ini.html
#

app = Celery(set_as_current=False)
app.config_from_object("t.integration.test_worker_config")


class PurgeMock:
    def queue_purge(self, queue):
        return 0


class ConnMock:
    default_channel = PurgeMock()
    channel_errors = KeyError


mock = Mock()
mock.__enter__ = Mock(return_value=ConnMock())
mock.__exit__ = Mock(return_value=False)

app.connection_for_write = MagicMock(return_value=mock)

# Below are taken from pyramid-celery's __init__.py
# Ref: https://github.com/sontek/pyramid_celery/blob/cf8aa80980e42f7235ad361874d3c35e19963b60/pyramid_celery/__init__.py#L25-L36 # noqa: E501
ini_option = Option(
    (
        "--ini",
        "-i",
    ),
    help="Paste ini configuration file.",
)

ini_var_option = Option(
    ("--ini-var",), help="Comma separated list of key=value to pass to ini."
)

app.user_options["preload"].add(ini_option)
app.user_options["preload"].add(ini_var_option)
