#!/usr/bin/env python3

import re
import sys

import click
import pyperclip
from colorama import Fore, init

# Initialize colorama for color support in terminal
init(autoreset=True)

# Regular expression pattern to match the required lines
PATTERN = re.compile(r"^\*\s*(.*?)\s+by\s+@[\w-]+\s+in\s+https://github\.com/[\w-]+/[\w-]+/pull/(\d+)")


def read_changes_file(filename):
    try:
        with open(filename) as f:
            return f.readlines()
    except FileNotFoundError:
        print(f"Error: {filename} file not found.")
        sys.exit(1)


def read_from_clipboard():
    text = pyperclip.paste()
    return text.splitlines()


def process_line(line):
    line = line.strip()

    # Skip lines containing '[pre-commit.ci]'
    if "[pre-commit.ci]" in line:
        return None

    # Skip lines starting with '## What's Changed'
    if line.startswith("## What's Changed"):
        return None

    # Stop processing if '## New Contributors' is encountered
    if line.startswith("## New Contributors"):
        return "STOP_PROCESSING"

    # Skip lines that don't start with '* '
    if not line.startswith("* "):
        return None

    match = PATTERN.match(line)
    if match:
        description, pr_number = match.groups()
        return f"- {description} (#{pr_number})"
    return None


@click.command()
@click.option(
    "--source",
    "-s",
    type=click.Path(exists=True),
    help="Source file to read from. If not provided, reads from clipboard.",
)
@click.option(
    "--dest",
    "-d",
    type=click.File("w"),
    default="-",
    help="Destination file to write to. Defaults to standard output.",
)
@click.option(
    "--clipboard",
    "-c",
    is_flag=True,
    help="Read input from clipboard explicitly.",
)
def main(source, dest, clipboard):
    # Determine the source of input
    if clipboard or (not source and not sys.stdin.isatty()):
        # Read from clipboard
        lines = read_from_clipboard()
    elif source:
        # Read from specified file
        lines = read_changes_file(source)
    else:
        # Default: read from clipboard
        lines = read_from_clipboard()

    output_lines = []
    for line in lines:
        output_line = process_line(line)
        if output_line == "STOP_PROCESSING":
            break
        if output_line:
            output_lines.append(output_line)

    output_text = "\n".join(output_lines)

    # Prepare the header
    version = "x.y.z"
    underline = "=" * len(version)

    header = f"""
.. _version-{version}:

{version}
{underline}

:release-date: <YYYY-MM-DD>
:release-by: <FULL NAME>

What's Changed
~~~~~~~~~~~~~~
"""

    # Combine header and output
    final_output = header + output_text

    # Write output to destination
    if dest.name == "<stdout>":
        print(Fore.GREEN + "Copy the following text to Changelog.rst:")
        print(Fore.YELLOW + header)
        print(Fore.CYAN + output_text)
    else:
        dest.write(final_output + "\n")
        dest.close()


if __name__ == "__main__":
    main()
