"""The ``celery result`` program, used to inspect task results."""
import click

from celery.bin.base import CeleryCommand, CeleryOption, handle_preload_options

try:
    from redis.exceptions import ConnectionError as RedisConnectionError
except ImportError:
    RedisConnectionError = None


@click.command(cls=CeleryCommand)
@click.argument('task_id')
@click.option('-t',
              '--task',
              cls=CeleryOption,
              help_group='Result Options',
              help="Name of task (if custom backend).")
@click.option('--traceback',
              cls=CeleryOption,
              is_flag=True,
              help_group='Result Options',
              help="Show traceback instead.")
@click.pass_context
@handle_preload_options
def result(ctx, task_id, task, traceback):
    """Print the return value for a given task id."""
    app = ctx.obj.app

    result_cls = app.tasks[task].AsyncResult if task else app.AsyncResult
    task_result = result_cls(task_id)
    try:
        value = task_result.traceback if traceback else task_result.get()
        ctx.obj.echo(value)
    except NotImplementedError:
        ctx.obj.echo("[Error] Celery result_backend is not configured. Cannot fetch task results.\nPlease add result_backend to your configuration, e.g.: result_backend='redis://localhost:6379/0'", err=True)
    except (ConnectionError, OSError) as exc:
        ctx.obj.echo(f"[Error] Cannot connect to result_backend: {exc}\nPlease check your configuration and ensure the backend service (e.g., Redis, database) is running.", err=True)
    except Exception as exc:
        # Also catch redis.exceptions.ConnectionError if available
        if RedisConnectionError and isinstance(exc, RedisConnectionError):
            ctx.obj.echo(f"[Error] Cannot connect to result_backend: {exc}\nPlease check your configuration and ensure the backend service (e.g., Redis, database) is running.", err=True)
        else:
            ctx.obj.echo(f"[Error] An unexpected error occurred while fetching task result: {exc}", err=True)
