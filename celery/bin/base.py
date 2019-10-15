"""Click customizations for Celery."""

from collections import OrderedDict

from celery._state import get_current_app

import click
from kombu.utils.objects import cached_property


class CLIContext:
    """Context Object for the CLI."""

    def __init__(self, app, no_color):
        """Initialize the CLI context."""
        self.app = app or get_current_app()
        self.no_color = no_color

    @cached_property
    def OK(self):
        return "OK" if self._no_color else click.style("OK", fg="green", bold=True)

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
