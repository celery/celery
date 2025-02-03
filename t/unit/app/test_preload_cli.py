import contextlib
from typing import Tuple
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from celery.bin.celery import celery


@pytest.fixture(autouse=True)
def reset_command_params_between_each_test():
    with contextlib.ExitStack() as stack:
        for command in celery.commands.values():
            # We only need shallow copy -- preload options are appended to the list,
            # existing options are kept as-is
            params_copy = command.params[:]
            patch_instance = patch.object(command, "params", params_copy)
            stack.enter_context(patch_instance)

        yield


@pytest.mark.parametrize(
    "subcommand_with_params",
    [
        ("purge", "-f"),
        ("shell",),
    ]
)
def test_preload_options(subcommand_with_params: Tuple[str, ...], isolated_cli_runner: CliRunner):
    # Verify commands like shell and purge can accept preload options.
    # Projects like Pyramid-Celery's ini option should be valid preload
    # options.
    res_without_preload = isolated_cli_runner.invoke(
        celery,
        ["-A", "t.unit.bin.proj.app", *subcommand_with_params, "--ini", "some_ini.ini"],
        catch_exceptions=False,
    )

    assert "No such option: --ini" in res_without_preload.stdout
    assert res_without_preload.exit_code == 2

    res_with_preload = isolated_cli_runner.invoke(
        celery,
        [
            "-A",
            "t.unit.bin.proj.pyramid_celery_app",
            *subcommand_with_params,
            "--ini",
            "some_ini.ini",
        ],
        catch_exceptions=False,
    )

    assert res_with_preload.exit_code == 0, res_with_preload.stdout
