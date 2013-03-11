#!/usr/bin/env python
from django.core.management import execute_manager
try:
    import settings             # Assumed to be in the same directory.
except ImportError:
    import sys
    sys.stderr.write(
        "Error: Can't find the file 'settings.py' in the directory "
        "containing {0!r}.".format(__file__))
    sys.exit(1)

if __name__ == '__main__':
    execute_manager(settings)
