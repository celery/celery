import sys

if sys.version_info < (3, 12):
    # due-work-harness needs Python 3.12 or later.
    collect_ignore_glob = ['*.py']
