"""Click customizations for Celery."""
import json
from collections import OrderedDict

import click
from click import ParamType
from kombu.utils.objects import cached_property

from celery._state import get_current_app
from celery.utils import text
from celery.utils.log import mlevel
from celery.utils.time import maybe_iso8601


class CLIContext:
    """Context Object for the CLI."""

    def __init__(self, app, no_color):
        """Initialize the CLI context."""
        self.app = app or get_current_app()
        self.no_color = no_color

    @cached_property
    def OK(self):
        return self.style("OK", fg="green", bold=True)

    def style(self, message=None, **kwargs):
        if self.no_color:
            return message
        else:
            return click.style(message, **kwargs)

    def secho(self, message=None, **kwargs):
        if self.no_color:
            kwargs.pop('color', None)
            click.echo(message, **kwargs)
        else:
            click.secho(message, **kwargs)

    def echo(self, message=None, **kwargs):
        if self.no_color:
            kwargs.pop('color', None)
            click.echo(message, **kwargs)
        else:
            click.echo(message, **kwargs)


class CeleryOption(click.Option):
    """Customized option for Celery."""

    def get_default(self, ctx):
        if self.default_value_from_context:
            self.default = ctx.obj[self.default_value_from_context]
        return super(CeleryOption, self).get_default(ctx)

    def __init__(self, *args, **kwargs):
        """Initialize a Celery option."""
        self.help_group = kwargs.pop('help_group', None)
        self.default_value_from_context = kwargs.pop('default_value_from_context', None)
        super(CeleryOption, self).__init__(*args, **kwargs)


class CeleryCommand(click.Command):
    """Customized command for Celery."""

    def format_options(self, ctx, formatter):
        """Write all the options into the formatter if they exist."""
        opts = OrderedDict()
        for param in self.get_params(ctx):
            rv = param.get_help_record(ctx)
            if rv is not None:
                if hasattr(param, 'help_group') and param.help_group:
                    opts.setdefault(str(param.help_group), []).append(rv)
                else:
                    opts.setdefault('Options', []).append(rv)

        for name, opts_group in opts.items():
            with formatter.section(name):
                formatter.write_dl(opts_group)


class CeleryDaemonCommand(CeleryCommand):
    """Daemon commands."""

    def __init__(self, *args, **kwargs):
        """Initialize a Celery command with common daemon options."""
        super().__init__(*args, **kwargs)
        self.params.append(CeleryOption(('-f', '--logfile'), help_group="Daemonization Options"))
        self.params.append(CeleryOption(('--pidfile',), help_group="Daemonization Options"))
        self.params.append(CeleryOption(('--uid',), help_group="Daemonization Options"))
        self.params.append(CeleryOption(('--uid',), help_group="Daemonization Options"))
        self.params.append(CeleryOption(('--gid',), help_group="Daemonization Options"))
        self.params.append(CeleryOption(('--umask',), help_group="Daemonization Options"))
        self.params.append(CeleryOption(('--executable',), help_group="Daemonization Options"))


class CommaSeparatedList(ParamType):
    """Comma separated list argument."""

    name = "comma separated list"

    def convert(self, value, param, ctx):
        return set(text.str_to_list(value))


COMMA_SEPARATED_LIST = CommaSeparatedList()


class Json(ParamType):
    """JSON formatted argument."""

    name = "json"

    def convert(self, value, param, ctx):
        try:
            return json.loads(value)
        except ValueError as e:
            self.fail(str(e))


JSON = Json()


class ISO8601DateTime(ParamType):
    """ISO 8601 Date Time argument."""

    name = "iso-86091"

    def convert(self, value, param, ctx):
        try:
            return maybe_iso8601(value)
        except (TypeError, ValueError) as e:
            self.fail(e)


class ISO8601DateTimeOrFloat(ParamType):
    """ISO 8601 Date Time or float argument."""

    name = "iso-86091 or float"

    def convert(self, value, param, ctx):
        try:
            return float(value)
        except (TypeError, ValueError):
            pass

        try:
            return maybe_iso8601(value)
        except (TypeError, ValueError) as e:
            self.fail(e)


ISO8601 = ISO8601DateTime()
ISO8601_OR_FLOAT = ISO8601DateTimeOrFloat()


class LogLevel(click.Choice):
    """Log level option."""

    def __init__(self):
        """Initialize the log level option with the relevant choices."""
        super().__init__(('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL'))

    def convert(self, value, param, ctx):
        value = super().convert(value, param, ctx)
        return mlevel(value)


LOG_LEVEL = LogLevel()
