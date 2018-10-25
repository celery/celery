.. _contributing:

==============
 Contributing
==============

Welcome!

This document is fairly extensive and you aren't really expected
to study this in detail for small contributions;

    The most important rule is that contributing must be easy
    and that the community is friendly and not nitpicking on details,
    such as coding style.

If you're reporting a bug you should read the Reporting bugs section
below to ensure that your bug report contains enough information
to successfully diagnose the issue, and if you're contributing code
you should try to mimic the conventions you see surrounding the code
you're working on, but in the end all patches will be cleaned up by
the person merging the changes so don't worry too much.

.. contents::
    :local:

.. _community-code-of-conduct:

Community Code of Conduct
=========================

The goal is to maintain a diverse community that's pleasant for everyone.
That's why we would greatly appreciate it if everyone contributing to and
interacting with the community also followed this Code of Conduct.

The Code of Conduct covers our behavior as members of the community,
in any forum, mailing list, wiki, website, Internet relay chat (IRC), public
meeting or private correspondence.

The Code of Conduct is heavily based on the `Ubuntu Code of Conduct`_, and
the `Pylons Code of Conduct`_.

.. _`Ubuntu Code of Conduct`: https://www.ubuntu.com/community/conduct
.. _`Pylons Code of Conduct`: http://docs.pylonshq.com/community/conduct.html

Be considerate
--------------

Your work will be used by other people, and you in turn will depend on the
work of others. Any decision you take will affect users and colleagues, and
we expect you to take those consequences into account when making decisions.
Even if it's not obvious at the time, our contributions to Celery will impact
the work of others. For example, changes to code, infrastructure, policy,
documentation and translations during a release may negatively impact
others work.

Be respectful
-------------

The Celery community and its members treat one another with respect. Everyone
can make a valuable contribution to Celery. We may not always agree, but
disagreement is no excuse for poor behavior and poor manners. We might all
experience some frustration now and then, but we cannot allow that frustration
to turn into a personal attack. It's important to remember that a community
where people feel uncomfortable or threatened isn't a productive one. We
expect members of the Celery community to be respectful when dealing with
other contributors as well as with people outside the Celery project and with
users of Celery.

Be collaborative
----------------

Collaboration is central to Celery and to the larger free software community.
We should always be open to collaboration. Your work should be done
transparently and patches from Celery should be given back to the community
when they're made, not just when the distribution releases. If you wish
to work on new code for existing upstream projects, at least keep those
projects informed of your ideas and progress. It many not be possible to
get consensus from upstream, or even from your colleagues about the correct
implementation for an idea, so don't feel obliged to have that agreement
before you begin, but at least keep the outside world informed of your work,
and publish your work in a way that allows outsiders to test, discuss, and
contribute to your efforts.

When you disagree, consult others
---------------------------------

Disagreements, both political and technical, happen all the time and
the Celery community is no exception. It's important that we resolve
disagreements and differing views constructively and with the help of the
community and community process. If you really want to go a different
way, then we encourage you to make a derivative distribution or alternate
set of packages that still build on the work we've done to utilize as common
of a core as possible.

When you're unsure, ask for help
--------------------------------

Nobody knows everything, and nobody is expected to be perfect. Asking
questions avoids many problems down the road, and so questions are
encouraged. Those who are asked questions should be responsive and helpful.
However, when asking a question, care must be taken to do so in an appropriate
forum.

Step down considerately
-----------------------

Developers on every project come and go and Celery is no different. When you
leave or disengage from the project, in whole or in part, we ask that you do
so in a way that minimizes disruption to the project. This means you should
tell people you're leaving and take the proper steps to ensure that others
can pick up where you leave off.

.. _reporting-bugs:


Reporting Bugs
==============

.. _vulnsec:

Security
--------

You must never report security related issues, vulnerabilities or bugs
including sensitive information to the bug tracker, or elsewhere in public.
Instead sensitive bugs must be sent by email to ``security@celeryproject.org``.

If you'd like to submit the information encrypted our PGP key is::

    -----BEGIN PGP PUBLIC KEY BLOCK-----
    Version: GnuPG v1.4.15 (Darwin)

    mQENBFJpWDkBCADFIc9/Fpgse4owLNvsTC7GYfnJL19XO0hnL99sPx+DPbfr+cSE
    9wiU+Wp2TfUX7pCLEGrODiEP6ZCZbgtiPgId+JYvMxpP6GXbjiIlHRw1EQNH8RlX
    cVxy3rQfVv8PGGiJuyBBjxzvETHW25htVAZ5TI1+CkxmuyyEYqgZN2fNd0wEU19D
    +c10G1gSECbCQTCbacLSzdpngAt1Gkrc96r7wGHBBSvDaGDD2pFSkVuTLMbIRrVp
    lnKOPMsUijiip2EMr2DvfuXiUIUvaqInTPNWkDynLoh69ib5xC19CSVLONjkKBsr
    Pe+qAY29liBatatpXsydY7GIUzyBT3MzgMJlABEBAAG0MUNlbGVyeSBTZWN1cml0
    eSBUZWFtIDxzZWN1cml0eUBjZWxlcnlwcm9qZWN0Lm9yZz6JATgEEwECACIFAlJp
    WDkCGwMGCwkIBwMCBhUIAgkKCwQWAgMBAh4BAheAAAoJEOArFOUDCicIw1IH/26f
    CViDC7/P13jr+srRdjAsWvQztia9HmTlY8cUnbmkR9w6b6j3F2ayw8VhkyFWgYEJ
    wtPBv8mHKADiVSFARS+0yGsfCkia5wDSQuIv6XqRlIrXUyqJbmF4NUFTyCZYoh+C
    ZiQpN9xGhFPr5QDlMx2izWg1rvWlG1jY2Es1v/xED3AeCOB1eUGvRe/uJHKjGv7J
    rj0pFcptZX+WDF22AN235WYwgJM6TrNfSu8sv8vNAQOVnsKcgsqhuwomSGsOfMQj
    LFzIn95MKBBU1G5wOs7JtwiV9jefGqJGBO2FAvOVbvPdK/saSnB+7K36dQcIHqms
    5hU4Xj0RIJiod5idlRC5AQ0EUmlYOQEIAJs8OwHMkrdcvy9kk2HBVbdqhgAREMKy
    gmphDp7prRL9FqSY/dKpCbG0u82zyJypdb7QiaQ5pfPzPpQcd2dIcohkkh7G3E+e
    hS2L9AXHpwR26/PzMBXyr2iNnNc4vTksHvGVDxzFnRpka6vbI/hrrZmYNYh9EAiv
    uhE54b3/XhXwFgHjZXb9i8hgJ3nsO0pRwvUAM1bRGMbvf8e9F+kqgV0yWYNnh6QL
    4Vpl1+epqp2RKPHyNQftbQyrAHXT9kQF9pPlx013MKYaFTADscuAp4T3dy7xmiwS
    crqMbZLzfrxfFOsNxTUGE5vmJCcm+mybAtRo4aV6ACohAO9NevMx8pUAEQEAAYkB
    HwQYAQIACQUCUmlYOQIbDAAKCRDgKxTlAwonCNFbB/9esir/f7TufE+isNqErzR/
    aZKZo2WzZR9c75kbqo6J6DYuUHe6xI0OZ2qZ60iABDEZAiNXGulysFLCiPdatQ8x
    8zt3DF9BMkEck54ZvAjpNSern6zfZb1jPYWZq3TKxlTs/GuCgBAuV4i5vDTZ7xK/
    aF+OFY5zN7ciZHkqLgMiTZ+RhqRcK6FhVBP/Y7d9NlBOcDBTxxE1ZO1ute6n7guJ
    ciw4hfoRk8qNN19szZuq3UU64zpkM2sBsIFM9tGF2FADRxiOaOWZHmIyVZriPFqW
    RUwjSjs7jBVNq0Vy4fCu/5+e+XLOUBOoqtM5W7ELt0t1w9tXebtPEetV86in8fU2
    =0chn
    -----END PGP PUBLIC KEY BLOCK-----

Other bugs
----------

Bugs can always be described to the :ref:`mailing-list`, but the best
way to report an issue and to ensure a timely response is to use the
issue tracker.

1) **Create a GitHub account**.

You need to `create a GitHub account`_ to be able to create new issues
and participate in the discussion.

.. _`create a GitHub account`: https://github.com/signup/free

2) **Determine if your bug is really a bug**.

You shouldn't file a bug if you're requesting support. For that you can use
the :ref:`mailing-list`, or :ref:`irc-channel`.

3) **Make sure your bug hasn't already been reported**.

Search through the appropriate Issue tracker. If a bug like yours was found,
check if you have new information that could be reported to help
the developers fix the bug.

4) **Check if you're using the latest version**.

A bug could be fixed by some other improvements and fixes - it might not have an
existing report in the bug tracker. Make sure you're using the latest releases of
celery, billiard, kombu, amqp, and vine.

5) **Collect information about the bug**.

To have the best chance of having a bug fixed, we need to be able to easily
reproduce the conditions that caused it. Most of the time this information
will be from a Python traceback message, though some bugs might be in design,
spelling or other errors on the website/docs/code.

    A) If the error is from a Python traceback, include it in the bug report.

    B) We also need to know what platform you're running (Windows, macOS, Linux,
       etc.), the version of your Python interpreter, and the version of Celery,
       and related packages that you were running when the bug occurred.

    C) If you're reporting a race condition or a deadlock, tracebacks can be
       hard to get or might not be that useful. Try to inspect the process to
       get more diagnostic data. Some ideas:

       * Enable Celery's :ref:`breakpoint signal <breakpoint_signal>` and use it
         to inspect the process's state. This will allow you to open a
         :mod:`pdb` session.
       * Collect tracing data using `strace`_(Linux),
         :command:`dtruss` (macOS), and :command:`ktrace` (BSD),
         `ltrace`_, and `lsof`_.

    D) Include the output from the :command:`celery report` command:

        .. code-block:: console

            $ celery -A proj report

        This will also include your configuration settings and it try to
        remove values for keys known to be sensitive, but make sure you also
        verify the information before submitting so that it doesn't contain
        confidential information like API tokens and authentication
        credentials.

6) **Submit the bug**.

By default `GitHub`_ will email you to let you know when new comments have
been made on your bug. In the event you've turned this feature off, you
should check back on occasion to ensure you don't miss any questions a
developer trying to fix the bug might ask.

.. _`GitHub`: https://github.com
.. _`strace`: https://en.wikipedia.org/wiki/Strace
.. _`ltrace`: https://en.wikipedia.org/wiki/Ltrace
.. _`lsof`: https://en.wikipedia.org/wiki/Lsof

.. _issue-trackers:

Issue Trackers
--------------

Bugs for a package in the Celery ecosystem should be reported to the relevant
issue tracker.

* :pypi:`celery`: https://github.com/celery/celery/issues/
* :pypi:`kombu`: https://github.com/celery/kombu/issues
* :pypi:`amqp`: https://github.com/celery/py-amqp/issues
* :pypi:`vine`: https://github.com/celery/vine/issues
* :pypi:`librabbitmq`: https://github.com/celery/librabbitmq/issues
* :pypi:`django-celery-beat`: https://github.com/celery/django-celery-beat/issues
* :pypi:`django-celery-results`: https://github.com/celery/django-celery-results/issues

If you're unsure of the origin of the bug you can ask the
:ref:`mailing-list`, or just use the Celery issue tracker.

Contributors guide to the code base
===================================

There's a separate section for internal details,
including details about the code base and a style guide.

Read :ref:`internals-guide` for more!

.. _versions:

Versions
========

Version numbers consists of a major version, minor version and a release number.
Since version 2.1.0 we use the versioning semantics described by
SemVer: http://semver.org.

Stable releases are published at PyPI
while development releases are only available in the GitHub git repository as tags.
All version tags starts with “v”, so version 0.8.0 is the tag v0.8.0.

.. _git-branches:

Branches
========

Current active version branches:

* dev (which git calls "master") (https://github.com/celery/celery/tree/master)
* 4.0 (https://github.com/celery/celery/tree/4.0)
* 3.1 (https://github.com/celery/celery/tree/3.1)
* 3.0 (https://github.com/celery/celery/tree/3.0)

You can see the state of any branch by looking at the Changelog:

    https://github.com/celery/celery/blob/master/Changelog

If the branch is in active development the topmost version info should
contain meta-data like:

.. code-block:: restructuredtext

    2.4.0
    ======
    :release-date: TBA
    :status: DEVELOPMENT
    :branch: dev (git calls this master)

The ``status`` field can be one of:

* ``PLANNING``

    The branch is currently experimental and in the planning stage.

* ``DEVELOPMENT``

    The branch is in active development, but the test suite should
    be passing and the product should be working and possible for users to test.

* ``FROZEN``

    The branch is frozen, and no more features will be accepted.
    When a branch is frozen the focus is on testing the version as much
    as possible before it is released.

dev branch
----------

The dev branch (called "master" by git), is where development of the next
version happens.

Maintenance branches
--------------------

Maintenance branches are named after the version -- for example,
the maintenance branch for the 2.2.x series is named ``2.2``.

Previously these were named ``releaseXX-maint``.

The versions we currently maintain is:

* 3.1

  This is the current series.

* 3.0

  This is the previous series, and the last version to support Python 2.5.

Archived branches
-----------------

Archived branches are kept for preserving history only,
and theoretically someone could provide patches for these if they depend
on a series that's no longer officially supported.

An archived version is named ``X.Y-archived``.

Our currently archived branches are:

* :github_branch:`2.5-archived`

* :github_branch:`2.4-archived`

* :github_branch:`2.3-archived`

* :github_branch:`2.1-archived`

* :github_branch:`2.0-archived`

* :github_branch:`1.0-archived`

Feature branches
----------------

Major new features are worked on in dedicated branches.
There's no strict naming requirement for these branches.

Feature branches are removed once they've been merged into a release branch.

Tags
====

- Tags are used exclusively for tagging releases. A release tag is
  named with the format ``vX.Y.Z`` -- for example ``v2.3.1``.

- Experimental releases contain an additional identifier ``vX.Y.Z-id`` --
  for example ``v3.0.0-rc1``.

- Experimental tags may be removed after the official release.

.. _contributing-changes:

Working on Features & Patches
=============================

.. note::

    Contributing to Celery should be as simple as possible,
    so none of these steps should be considered mandatory.

    You can even send in patches by email if that's your preferred
    work method. We won't like you any less, any contribution you make
    is always appreciated!

    However following these steps may make maintainers life easier,
    and may mean that your changes will be accepted sooner.

Forking and setting up the repository
-------------------------------------

First you need to fork the Celery repository, a good introduction to this
is in the GitHub Guide: `Fork a Repo`_.

After you have cloned the repository you should checkout your copy
to a directory on your machine:

.. code-block:: console

    $ git clone git@github.com:username/celery.git

When the repository is cloned enter the directory to set up easy access
to upstream changes:

.. code-block:: console

    $ cd celery
    $ git remote add upstream git://github.com/celery/celery.git
    $ git fetch upstream

If you need to pull in new changes from upstream you should
always use the ``--rebase`` option to ``git pull``:

.. code-block:: console

    git pull --rebase upstream master

With this option you don't clutter the history with merging
commit notes. See `Rebasing merge commits in git`_.
If you want to learn more about rebasing see the `Rebase`_
section in the GitHub guides.

If you need to work on a different branch than the one git calls ``master``, you can
fetch and checkout a remote branch like this::

    git checkout --track -b 3.0-devel origin/3.0-devel

.. _`Fork a Repo`: https://help.github.com/fork-a-repo/
.. _`Rebasing merge commits in git`:
    https://notes.envato.com/developers/rebasing-merge-commits-in-git/
.. _`Rebase`: https://help.github.com/rebase/

.. _contributing-docker-development:

Developing and Testing with Docker
----------------------------------

Because of the many components of Celery, such as a broker and backend,
`Docker`_ and `docker-compose`_ can be utilized to greatly simplify the
development and testing cycle. The Docker configuration here requires a
Docker version of at least 17.09.

The Docker components can be found within the :file:`docker/` folder and the
Docker image can be built via:

.. code-block:: console

    $ docker-compose build celery

and run via:

.. code-block:: console

    $ docker-compose run --rm celery <command>

where <command> is a command to execute in a Docker container. The `--rm` flag
indicates that the container should be removed after it is exited and is useful
to prevent accumulation of unwanted containers.

Some useful commands to run:

* ``bash``

    To enter the Docker container like a normal shell

* ``make test``

    To run the test suite

* ``tox``

    To run tox and test against a variety of configurations

By default, docker-compose will mount the Celery and test folders in the Docker
container, allowing code changes and testing to be immediately visible inside
the Docker container. Environment variables, such as the broker and backend to
use are also defined in the :file:`docker/docker-compose.yml` file.

.. _`Docker`: https://www.docker.com/
.. _`docker-compose`: https://docs.docker.com/compose/

.. _contributing-testing:

Running the unit test suite
---------------------------

To run the Celery test suite you need to install a few dependencies.
A complete list of the dependencies needed are located in
:file:`requirements/test.txt`.

If you're working on the development version, then you need to
install the development requirements first:

.. code-block:: console

    $ pip install -U -r requirements/dev.txt

THIS REQUIREMENT FILE MAY NOT BE PRESENT, SKIP IF NOT FOUND.

Both the stable and the development version have testing related
dependencies, so install these next:

.. code-block:: console

    $ pip install -U -r requirements/test.txt
    $ pip install -U -r requirements/default.txt

After installing the dependencies required, you can now execute
the test suite by calling :pypi:`py.test <pytest>`:

.. code-block:: console

    $ py.test

Some useful options to :command:`py.test` are:

* ``-x``

    Stop running the tests at the first test that fails.

* ``-s``

    Don't capture output

* ``-v``

    Run with verbose output.

If you want to run the tests for a single test file only
you can do so like this:

.. code-block:: console

    $ py.test t/unit/worker/test_worker.py

.. _contributing-pull-requests:

Creating pull requests
----------------------

When your feature/bugfix is complete you may want to submit
a pull requests so that it can be reviewed by the maintainers.

Creating pull requests is easy, and also let you track the progress
of your contribution. Read the `Pull Requests`_ section in the GitHub
Guide to learn how this is done.

You can also attach pull requests to existing issues by following
the steps outlined here: https://bit.ly/koJoso

.. _`Pull Requests`: http://help.github.com/send-pull-requests/

.. _contributing-coverage:

Calculating test coverage
~~~~~~~~~~~~~~~~~~~~~~~~~

To calculate test coverage you must first install the :pypi:`pytest-cov` module.

Installing the :pypi:`pytest-cov` module:

.. code-block:: console

    $ pip install -U pytest-cov

Code coverage in HTML format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Run :command:`py.test` with the ``--cov-report=html`` argument enabled:

    .. code-block:: console

        $ py.test --cov=celery --cov-report=html

#. The coverage output will then be located in the :file:`htmlcov/` directory:

    .. code-block:: console

        $ open htmlcov/index.html

Code coverage in XML (Cobertura-style)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. Run :command:`py.test` with the ``--cov-report=xml`` argument enabled:

.. code-block:: console

    $ py.test --cov=celery --cov-report=xml

#. The coverage XML output will then be located in the :file:`coverage.xml` file.

.. _contributing-tox:

Running the tests on all supported Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There's a :pypi:`tox` configuration file in the top directory of the
distribution.

To run the tests for all supported Python versions simply execute:

.. code-block:: console

    $ tox

Use the ``tox -e`` option if you only want to test specific Python versions:

.. code-block:: console

    $ tox -e 2.7

Building the documentation
--------------------------

To build the documentation you need to install the dependencies
listed in :file:`requirements/docs.txt` and :file:`requirements/default.txt`:

.. code-block:: console

    $ pip install -U -r requirements/docs.txt
    $ pip install -U -r requirements/default.txt

Additionally, to build with no warnings, you will need to install
the following packages:

.. code-block:: console

   $ apt-get install texlive texlive-latex-extra dvipng

After these dependencies are installed you should be able to
build the docs by running:

.. code-block:: console

    $ cd docs
    $ rm -rf _build
    $ make html

Make sure there are no errors or warnings in the build output.
After building succeeds the documentation is available at :file:`_build/html`.

.. _contributing-verify:

Verifying your contribution
---------------------------

To use these tools you need to install a few dependencies. These dependencies
can be found in :file:`requirements/pkgutils.txt`.

Installing the dependencies:

.. code-block:: console

    $ pip install -U -r requirements/pkgutils.txt

pyflakes & PEP-8
~~~~~~~~~~~~~~~~

To ensure that your changes conform to :pep:`8` and to run pyflakes
execute:

.. code-block:: console

    $ make flakecheck

To not return a negative exit code when this command fails use
the ``flakes`` target instead:

.. code-block:: console

    $ make flakes

API reference
~~~~~~~~~~~~~

To make sure that all modules have a corresponding section in the API
reference please execute:

.. code-block:: console

    $ make apicheck
    $ make indexcheck

If files are missing you can add them by copying an existing reference file.

If the module is internal it should be part of the internal reference
located in :file:`docs/internals/reference/`. If the module is public
it should be located in :file:`docs/reference/`.

For example if reference is missing for the module ``celery.worker.awesome``
and this module is considered part of the public API, use the following steps:


Use an existing file as a template:

.. code-block:: console

    $ cd docs/reference/
    $ cp celery.schedules.rst celery.worker.awesome.rst

Edit the file using your favorite editor:

.. code-block:: console

    $ vim celery.worker.awesome.rst

        # change every occurrence of ``celery.schedules`` to
        # ``celery.worker.awesome``


Edit the index using your favorite editor:

.. code-block:: console

    $ vim index.rst

        # Add ``celery.worker.awesome`` to the index.


Commit your changes:

.. code-block:: console

    # Add the file to git
    $ git add celery.worker.awesome.rst
    $ git add index.rst
    $ git commit celery.worker.awesome.rst index.rst \
        -m "Adds reference for celery.worker.awesome"

.. _coding-style:

Coding Style
============

You should probably be able to pick up the coding style
from surrounding code, but it is a good idea to be aware of the
following conventions.

* All Python code must follow the :pep:`8` guidelines.

:pypi:`pep8` is a utility you can use to verify that your code
is following the conventions.

* Docstrings must follow the :pep:`257` conventions, and use the following
  style.

    Do this:

    .. code-block:: python

        def method(self, arg):
            """Short description.

            More details.

            """

    or:

    .. code-block:: python

        def method(self, arg):
            """Short description."""


    but not this:

    .. code-block:: python

        def method(self, arg):
            """
            Short description.
            """

* Lines shouldn't exceed 78 columns.

  You can enforce this in :command:`vim` by setting the ``textwidth`` option:

  .. code-block:: vim

        set textwidth=78

  If adhering to this limit makes the code less readable, you have one more
  character to go on. This means 78 is a soft limit, and 79 is the hard
  limit :)

* Import order

    * Python standard library (`import xxx`)
    * Python standard library (`from xxx import`)
    * Third-party packages.
    * Other modules from the current package.

    or in case of code using Django:

    * Python standard library (`import xxx`)
    * Python standard library (`from xxx import`)
    * Third-party packages.
    * Django packages.
    * Other modules from the current package.

    Within these sections the imports should be sorted by module name.

    Example:

    .. code-block:: python

        import threading
        import time

        from collections import deque
        from Queue import Queue, Empty

        from .platforms import Pidfile
        from .five import zip_longest, items, range
        from .utils.time import maybe_timedelta

* Wild-card imports must not be used (`from xxx import *`).

* For distributions where Python 2.5 is the oldest support version
  additional rules apply:

    * Absolute imports must be enabled at the top of every module::

        from __future__ import absolute_import

    * If the module uses the :keyword:`with` statement and must be compatible
      with Python 2.5 (celery isn't) then it must also enable that::

        from __future__ import with_statement

    * Every future import must be on its own line, as older Python 2.5
      releases didn't support importing multiple features on the
      same future import line::

        # Good
        from __future__ import absolute_import
        from __future__ import with_statement

        # Bad
        from __future__ import absolute_import, with_statement

     (Note that this rule doesn't apply if the package doesn't include
     support for Python 2.5)


* Note that we use "new-style" relative imports when the distribution
  doesn't support Python versions below 2.5

    This requires Python 2.5 or later:

    .. code-block:: python

        from . import submodule


.. _feature-with-extras:

Contributing features requiring additional libraries
====================================================

Some features like a new result backend may require additional libraries
that the user must install.

We use setuptools `extra_requires` for this, and all new optional features
that require third-party libraries must be added.

1) Add a new requirements file in `requirements/extras`

    For the Cassandra backend this is
    :file:`requirements/extras/cassandra.txt`, and the file looks like this:

    .. code-block:: text

        pycassa

    These are pip requirement files so you can have version specifiers and
    multiple packages are separated by newline. A more complex example could
    be:

    .. code-block:: text

        # pycassa 2.0 breaks Foo
        pycassa>=1.0,<2.0
        thrift

2) Modify ``setup.py``

    After the requirements file is added you need to add it as an option
    to :file:`setup.py` in the ``extras_require`` section::

        extra['extras_require'] = {
            # ...
            'cassandra': extras('cassandra.txt'),
        }

3) Document the new feature in :file:`docs/includes/installation.txt`

    You must add your feature to the list in the :ref:`bundles` section
    of :file:`docs/includes/installation.txt`.

    After you've made changes to this file you need to render
    the distro :file:`README` file:

    .. code-block:: console

        $ pip install -U requirements/pkgutils.txt
        $ make readme


That's all that needs to be done, but remember that if your feature
adds additional configuration options then these needs to be documented
in :file:`docs/configuration.rst`. Also all settings need to be added to the
:file:`celery/app/defaults.py` module.

Result backends require a separate section in the :file:`docs/configuration.rst`
file.

.. _contact_information:

Contacts
========

This is a list of people that can be contacted for questions
regarding the official git repositories, PyPI packages
Read the Docs pages.

If the issue isn't an emergency then it's better
to :ref:`report an issue <reporting-bugs>`.


Committers
----------

Ask Solem
~~~~~~~~~

:github: https://github.com/ask
:twitter: https://twitter.com/#!/asksol

Asif Saif Uddin
~~~~~~~~~~~~~~~

:github: https://github.com/auvipy
:twitter: https://twitter.com/#!/auvipy

Dmitry Malinovsky
~~~~~~~~~~~~~~~~~

:github: https://github.com/malinoff
:twitter: https://twitter.com/__malinoff__

Ionel Cristian Mărieș
~~~~~~~~~~~~~~~~~~~~~

:github: https://github.com/ionelmc
:twitter: https://twitter.com/ionelmc

Mher Movsisyan
~~~~~~~~~~~~~~

:github: https://github.com/mher
:twitter: https://twitter.com/#!/movsm

Omer Katz
~~~~~~~~~
:github: https://github.com/thedrow
:twitter: https://twitter.com/the_drow

Steeve Morin
~~~~~~~~~~~~

:github: https://github.com/steeve
:twitter: https://twitter.com/#!/steeve

Website
-------

The Celery Project website is run and maintained by

Mauro Rocco
~~~~~~~~~~~

:github: https://github.com/fireantology
:twitter: https://twitter.com/#!/fireantology

with design by:

Jan Henrik Helmers
~~~~~~~~~~~~~~~~~~

:web: http://www.helmersworks.com
:twitter: https://twitter.com/#!/helmers


.. _packages:

Packages
========

``celery``
----------

:git: https://github.com/celery/celery
:CI: https://travis-ci.org/#!/celery/celery
:Windows-CI: https://ci.appveyor.com/project/ask/celery
:PyPI: :pypi:`celery`
:docs: http://docs.celeryproject.org

``kombu``
---------

Messaging library.

:git: https://github.com/celery/kombu
:CI: https://travis-ci.org/#!/celery/kombu
:Windows-CI: https://ci.appveyor.com/project/ask/kombu
:PyPI: :pypi:`kombu`
:docs: https://kombu.readthedocs.io

``amqp``
--------

Python AMQP 0.9.1 client.

:git: https://github.com/celery/py-amqp
:CI: https://travis-ci.org/#!/celery/py-amqp
:Windows-CI: https://ci.appveyor.com/project/ask/py-amqp
:PyPI: :pypi:`amqp`
:docs: https://amqp.readthedocs.io

``vine``
--------

Promise/deferred implementation.

:git: https://github.com/celery/vine/
:CI: https://travis-ci.org/#!/celery/vine/
:Windows-CI: https://ci.appveyor.com/project/ask/vine
:PyPI: :pypi:`vine`
:docs: https://vine.readthedocs.io

``billiard``
------------

Fork of multiprocessing containing improvements
that'll eventually be merged into the Python stdlib.

:git: https://github.com/celery/billiard
:CI: https://travis-ci.org/#!/celery/billiard/
:Windows-CI: https://ci.appveyor.com/project/ask/billiard
:PyPI: :pypi:`billiard`

``django-celery-beat``
----------------------

Database-backed Periodic Tasks with admin interface using the Django ORM.

:git: https://github.com/celery/django-celery-beat
:CI: https://travis-ci.org/#!/celery/django-celery-beat
:Windows-CI: https://ci.appveyor.com/project/ask/django-celery-beat
:PyPI: :pypi:`django-celery-beat`

``django-celery-results``
-------------------------

Store task results in the Django ORM, or using the Django Cache Framework.

:git: https://github.com/celery/django-celery-results
:CI: https://travis-ci.org/#!/celery/django-celery-results
:Windows-CI: https://ci.appveyor.com/project/ask/django-celery-results
:PyPI: :pypi:`django-celery-results`

``librabbitmq``
---------------

Very fast Python AMQP client written in C.

:git: https://github.com/celery/librabbitmq
:PyPI: :pypi:`librabbitmq`

``cell``
--------

Actor library.

:git: https://github.com/celery/cell
:PyPI: :pypi:`cell`

``cyme``
--------

Distributed Celery Instance manager.

:git: https://github.com/celery/cyme
:PyPI: :pypi:`cyme`
:docs: https://cyme.readthedocs.io/


Deprecated
----------

- ``django-celery``

:git: https://github.com/celery/django-celery
:PyPI: :pypi:`django-celery`
:docs: http://docs.celeryproject.org/en/latest/django

- ``Flask-Celery``

:git: https://github.com/ask/Flask-Celery
:PyPI: :pypi:`Flask-Celery`

- ``celerymon``

:git: https://github.com/celery/celerymon
:PyPI: :pypi:`celerymon`

- ``carrot``

:git: https://github.com/ask/carrot
:PyPI: :pypi:`carrot`

- ``ghettoq``

:git: https://github.com/ask/ghettoq
:PyPI: :pypi:`ghettoq`

- ``kombu-sqlalchemy``

:git: https://github.com/ask/kombu-sqlalchemy
:PyPI: :pypi:`kombu-sqlalchemy`

- ``django-kombu``

:git: https://github.com/ask/django-kombu
:PyPI: :pypi:`django-kombu`

- ``pylibrabbitmq``

Old name for :pypi:`librabbitmq`.

:git: :const:`None`
:PyPI: :pypi:`pylibrabbitmq`

.. _release-procedure:


Release Procedure
=================

Updating the version number
---------------------------

The version number must be updated two places:

    * :file:`celery/__init__.py`
    * :file:`docs/include/introduction.txt`

After you have changed these files you must render
the :file:`README` files. There's a script to convert sphinx syntax
to generic reStructured Text syntax, and the make target `readme`
does this for you:

.. code-block:: console

    $ make readme

Now commit the changes:

.. code-block:: console

    $ git commit -a -m "Bumps version to X.Y.Z"

and make a new version tag:

.. code-block:: console

    $ git tag vX.Y.Z
    $ git push --tags

Releasing
---------

Commands to make a new public stable release:

.. code-block:: console

    $ make distcheck  # checks pep8, autodoc index, runs tests and more
    $ make dist  # NOTE: Runs git clean -xdf and removes files not in the repo.
    $ python setup.py sdist upload --sign --identity='Celery Security Team'
    $ python setup.py bdist_wheel upload --sign --identity='Celery Security Team'

If this is a new release series then you also need to do the
following:

* Go to the Read The Docs management interface at:
    https://readthedocs.org/projects/celery/?fromdocs=celery

* Enter "Edit project"

    Change default branch to the branch of this series, for example, use
    the ``2.4`` branch for the 2.4 series.

* Also add the previous version under the "versions" tab.

.. _`mailing-list`: https://groups.google.com/group/celery-users

.. _`irc-channel`: http://docs.celeryproject.org/en/latest/getting-started/resources.html#irc

.. _`internals-guide`: http://docs.celeryproject.org/en/latest/internals/guide.html

.. _`bundles`: http://docs.celeryproject.org/en/latest/getting-started/introduction.html#bundles

.. _`report an issue`: http://docs.celeryproject.org/en/latest/contributing.html#reporting-bugs

