======================================
 Example Python project using Celery
======================================


Modules
-------

    * celeryconfig.py

        The celery configuration module.

    * tasks.py

        Tasks are defined in this module. This module is automatically
        imported by the worker because it's listed in
        celeryconfig's `CELERY_IMPORTS` directive.

    * test.py

        Simple test program running tasks.



Running
-------


Open up two terminals, in the first you run:

    $ celeryd --loglevel=INFO

In the other you run the test program:

    $ python ./test.py

Voila, you've executed some tasks!
