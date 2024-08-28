========================
 pip requirements files
========================


Index
=====

* :file:`requirements/default.txt`

    Default requirements for Python 3.8+.


* :file:`requirements/security.txt`

    Extra requirements needed to use the message signing serializer,
    see the Security Guide.

* :file:`requirements/test.txt`

    Requirements needed to run the full unittest suite.

* :file:`requirements/test-ci-base.txt`

    Extra test requirements required by the CI suite (Tox).

* :file:`requirements/test-ci-default.txt`

    Extra test requirements required for Python 3.8 by the CI suite (Tox).

* :file:`requirements/test-integration.txt`

    Extra requirements needed when running the integration test suite.

* :file:`requirements/doc.txt`

    Extra requirements required to build the Sphinx documentation.

* :file:`requirements/pkgutils.txt`

    Extra requirements required to perform package distribution maintenance.

* :file:`requirements/dev.txt`

    Requirement file installing the current dev branch of Celery and
    dependencies (will not be present in stable branches).

Examples
========

Installing requirements
-----------------------

::

    $ pip install -U -r requirements/default.txt


Running the tests
-----------------

::

    $ pip install -U -r requirements/default.txt
    $ pip install -U -r requirements/test.txt
