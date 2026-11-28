import sys

if sys.version_info < (3, 11):
    # pytest-obligation needs Python 3.11 or later.
    collect_ignore_glob = ['*.py']
