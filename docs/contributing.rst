.. _contributing:

==============
 Contributing
==============

.. contents::
    :local:

.. _community-code-of-conduct:

Community Code of Conduct
=========================

The goal is to maintain a diverse community that is pleasant for everyone.
That is why we would greatly appreciate it if everyone contributing to and
interacting with the community also followed this Code of Conduct.

The Code of Conduct covers our behavior as members of the community,
in any forum, mailing list, wiki, website, Internet relay chat (IRC), public
meeting or private correspondence.

The Code of Conduct is heavily based on the `Ubuntu Code of Conduct`_, and
the `Pylons Code of Conduct`_.

.. _`Ubuntu Code of Conduct`: http://www.ubuntu.com/community/conduct
.. _`Pylons Code of Conduct`: http://docs.pylonshq.com/community/conduct.html

Be considerate.
---------------

Your work will be used by other people, and you in turn will depend on the
work of others.  Any decision you take will affect users and colleagues, and
we expect you to take those consequences into account when making decisions.
Even if it's not obvious at the time, our contributions to Ubuntu will impact
the work of others.  For example, changes to code, infrastructure, policy,
documentation and translations during a release may negatively impact
others work.

Be respectful.
--------------

The Celery community and its members treat one another with respect.  Everyone
can make a valuable contribution to Celery.  We may not always agree, but
disagreement is no excuse for poor behavior and poor manners.  We might all
experience some frustration now and then, but we cannot allow that frustration
to turn into a personal attack.  It's important to remember that a community
where people feel uncomfortable or threatened is not a productive one.  We
expect members of the Celery community to be respectful when dealing with
other contributors as well as with people outside the Celery project and with
users of Celery.

Be collaborative.
-----------------

Collaboration is central to Celery and to the larger free software community.
We should always be open to collaboration.  Your work should be done
transparently and patches from Celery should be given back to the community
when they are made, not just when the distribution releases.  If you wish
to work on new code for existing upstream projects, at least keep those
projects informed of your ideas and progress.  It many not be possible to
get consensus from upstream, or even from your colleagues about the correct
implementation for an idea, so don't feel obliged to have that agreement
before you begin, but at least keep the outside world informed of your work,
and publish your work in a way that allows outsiders to test, discuss and
contribute to your efforts.

When you disagree, consult others.
----------------------------------

Disagreements, both political and technical, happen all the time and
the Celery community is no exception.  It is important that we resolve
disagreements and differing views constructively and with the help of the
community and community process.  If you really want to go a different
way, then we encourage you to make a derivative distribution or alternate
set of packages that still build on the work we've done to utilize as common
of a core as possible.

When you are unsure, ask for help.
----------------------------------

Nobody knows everything, and nobody is expected to be perfect.  Asking
questions avoids many problems down the road, and so questions are
encouraged.  Those who are asked questions should be responsive and helpful.
However, when asking a question, care must be taken to do so in an appropriate
forum.

Step down considerately.
------------------------

Developers on every project come and go and Celery is no different.  When you
leave or disengage from the project, in whole or in part, we ask that you do
so in a way that minimizes disruption to the project.  This means you should
tell people you are leaving and take the proper steps to ensure that others
can pick up where you leave off.

.. _reporting-bugs:

Reporting a Bug
===============

Bugs can always be described to the :ref:`mailing-list`, but the best
way to report an issue and to ensure a timely response is to use the
issue tracker.

1) Create a GitHub account.

You need to `create a GitHub account`_ to be able to create new issues
and participate in the discussion.

.. _`create a GitHub account`: https://github.com/signup/free

2) Determine if your bug is really a bug.

You should not file a bug if you are requesting support.  For that you can use
the :ref:`mailing-list`, or :ref:`irc-channel`.

3) Make sure your bug hasn't already been reported.

Search through the appropriate Issue tracker.  If a bug like yours was found,
check if you have new information that could be reported to help
the developers fix the bug.

4) Collect information about the bug.

To have the best chance of having a bug fixed, we need to be able to easily
reproduce the conditions that caused it.  Most of the time this information
will be from a Python traceback message, though some bugs might be in design,
spelling or other errors on the website/docs/code.

If the error is from a Python traceback, include it in the bug report.

We also need to know what platform you're running (Windows, OSX, Linux, etc),
the version of your Python interpreter, and the version of Celery, and related
packages that you were running when the bug occurred.

5) Submit the bug.

By default `GitHub`_ will email you to let you know when new comments have
been made on your bug. In the event you've turned this feature off, you
should check back on occasion to ensure you don't miss any questions a
developer trying to fix the bug might ask.

.. _`GitHub`: http://github.com

.. _issue-trackers:

Issue Trackers
--------------

Bugs for a package in the Celery ecosystem should be reported to the relevant
issue tracker.

* Celery: http://github.com/ask/celery/issues/
* Django-Celery: http://github.com/ask/django-celery/issues
* Flask-Celery: http://github.com/ask/flask-celery/issues
* Celery-Pylons: http://bitbucket.org/ianschenck/celery-pylons/issues
* Kombu: http://github.com/ask/kombu/issues

If you are unsure of the origin of the bug you can ask the
:ref:`mailing-list`, or just use the Celery issue tracker.

.. _versions:

Versions
========

Version numbers consists of a major version, minor version and a release number.
Since version 2.1.0 we use the versioning semantics described by
semver: http://semver.org.

Stable releases are published at PyPI
while development releases are only available in the GitHub git repository as tags.
All version tags starts with “v”, so version 0.8.0 is the tag v0.8.0.

.. _git-branches:

Branches
========

Current active version branches:

* master (http://github.com/ask/celery/tree/master)
* 3.0-devel (http://github.com/ask/celery/tree/3.0-devel)

You can see the state of any branch by looking at the Changelog:

    https://github.com/ask/celery/blob/master/Changelog

If the branch is in active development the topmost version info should
contain metadata like::

    2.4.0
    ======
    :release-date: TBA
    :status: DEVELOPMENT
    :branch: master

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

``master`` branch
-----------------

The master branch is where development of the next version happens.

Maintenance branches
--------------------

Maintenance branches are named after the version, e.g. the maintenance branch
for the 2.2.x series is named ``2.2``.  Previously these were named
``releaseXX-maint``.

The versions we currently maintain is:

* 2.3

  This is the current series.

* 2.2

  This is the previous series, and the last version to support Python 2.4.

* 2.1

  This is the last version to use the ``carrot`` AMQP framework.
  Recent versions use ``kombu``.

Archived branches
-----------------

Archived branches are kept for preserving history only,
and theoretically someone could provide patches for these if they depend
on a series that is no longer officially supported.

An archived version is named ``X.Y-archived``.

Our currently archived branches are:

* 2.1-archived

* 2.0-archived

* 1.0-archived

Feature branches
----------------

Major new features are worked on in dedicated branches.
There is no strict naming requirement for these branches.

Feature branches are removed once they have been merged into a release branch.

Tags
====

Tags are used exclusively for tagging releases.  A release tag is
named with the format ``vX.Y.Z``, e.g. ``v2.3.1``.
Experimental releases contain an additional identifier ``vX.Y.Z-id``, e.g.
``v3.0.0-rc1``.  Experimental tags may be removed after the official release.

.. _contributing-changes:

Working on Features & Patches
=============================

.. note::

    Contributing to Celery should be as simple as possible,
    so none of these steps should be considered mandatory.

    You can even send in patches by email if that is your preferred
    work method. We won't like you any less, any contribution you make
    is always appreciated!

    However following these steps may make maintainers life easier,
    and may mean that your changes will be accepted sooner.

Forking and setting up the repository
-------------------------------------

First you need to fork the Celery repository, a good introduction to this
is in the Github Guide: `Fork a Repo`_.

After you have cloned the repository you should checkout your copy
to a directory on your machine::

    $ git clone git@github.com:username/celery.git

When the repository is cloned enter the directory to set up easy access
to upstream changes::

    $ cd celery
    $ git remote add upstream git://github.com/ask/celery.git
    $ git fetch upstream

If you need to pull in new changes from upstream you should
always use the :option:`--rebase` option to ``git pull``::

    git pull --rebase upstream master

With this option you don't clutter the history with merging
commit notes. See `Rebasing merge commits in git`_.
If you want to learn more about rebasing see the `Rebase`_
section in the Github guides.

If you need to work on a different branch than ``master`` you can
fetch and checkout a remote branch like this::

    git checkout --track -b 3.0-devel origin/3.0-devel

For a list of branches see :ref:`git-branches`.

.. _`Fork a Repo`: http://help.github.com/fork-a-repo/
.. _`Rebasing merge commits in git`:
    http://notes.envato.com/developers/rebasing-merge-commits-in-git/
.. _`Rebase`: http://help.github.com/rebase/

.. _contributing-testing:

Running the unit test suite
---------------------------

To run the Celery test suite you need to install a few dependencies.
A complete list of the dependencies needed are located in
:file:`requirements/test.txt`.

Installing the test requirements::

    $ pip -E $VIRTUAL_ENV install -U -r requirements/test.txt

When installation of dependencies is complete you can execute
the test suite by calling ``nosetests``::

    $ nosetests

Some useful options to :program:`nosetests` are:

* :option:`-x`

    Stop running the tests at the first test that fails.

* :option:`-s`

    Don't capture output

* :option:`--nologcapture`

    Don't capture log output.

* :option:`-v`

    Run with verbose output.

If you want to run the tests for a single test file only
you can do so like this::

    $ nosetests celery.tests.test_worker.test_worker_job

.. _contributing-pull-requests:

Creating pull requests
----------------------

When your feature/bugfix is complete you may want to submit
a pull requests so that it can be reviewed by the maintainers.

Creating pull requests is easy, and also let you track the progress
of your contribution.  Read the `Pull Requests`_ section in the Github
Guide to learn how this is done.

You can also attach pull requests to existing issues by following
the steps outlined here: http://bit.ly/koJoso

.. _`Pull Requests`: http://help.github.com/send-pull-requests/

.. _contributing-coverage:

Calculating test coverage
~~~~~~~~~~~~~~~~~~~~~~~~~

Code coverage in HTML::

    $ nosetests --with-coverage3 --cover3-html

The coverage output will then be located at
:file:`celery/tests/cover/index.html`.

Code coverage in XML (Cobertura-style)::

    $ nosetests --with-coverage3 --cover3-xml --cover3-xml-file=coverage.xml

The coverage XML output will then be located at :file:`coverage.xml`

.. _contributing-tox:

Running the tests on all supported Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There is a ``tox`` configuration file in the top directory of the
distribution.

To run the tests for all supported Python versions simply execute::

    $ tox

If you only want to test specific Python versions use the :option:`-e`
option::

    $ tox -e py25,py26

Building the documentation
--------------------------

To build the documentation you need to install the dependencies
listed in :file:`requirements/docs.txt`::

    $ pip -E $VIRTUAL_ENV install -U -r requirements/docs.txt

After these dependencies are installed you should be able to
build the docs by running::

    $ cd docs
    $ rm -rf .build
    $ make html

Make sure there are no errors or warnings in the build output.
After building succeeds the documentation is available at :file:`.build/html`.

.. _contributing-verify:

Verifying your contribution
---------------------------

To use these tools you need to install a few dependencies.  These dependencies
can be found in :file:`requirements/pkgutils.txt`.

Installing the dependencies::

    $ pip -E $VIRTUAL_ENV install -U -r requirements/pkgutils.txt

pyflakes & PEP8
~~~~~~~~~~~~~~~

To ensure that your changes conform to PEP8 and to run pyflakes
execute::

    $ paver flake8

To not return a negative exit code when this command fails use the
:option:`-E` option, this can be convenient while developing::

    $ paver flake8 -E

API reference
~~~~~~~~~~~~~

To make sure that all modules have a corresponding section in the API
reference please execute::

    $ paver autodoc
    $ paver verifyindex

If files are missing you can add them by copying an existing reference file.

If the module is internal it should be part of the internal reference
located in :file:`docs/internals/reference/`.  If the module is public
it should be located in :file:`docs/reference/`.

For example if reference is missing for the module ``celery.worker.awesome``
and this module is considered part of the public API, use the following steps::

    $ cd docs/reference/
    $ cp celery.schedules.rst celery.worker.awesome.rst
    $ vim celery.worker.awesome.rst

        # change every occurance of ``celery.schedules`` to
        # ``celery.worker.awesome``

    $ vim index.rst

        # Add ``celery.worker.awesome`` to the index.

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

* All Python code must follow the `PEP-8`_ guidelines.

`pep8.py`_ is an utility you can use to verify that your code
is following the conventions.

.. _`PEP-8`: http://www.python.org/dev/peps/pep-0008/
.. _`pep8.py`: http://pypi.python.org/pypi/pep8

* Docstrings must follow the `PEP-257`_ conventions, and use the following
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

.. _`PEP-257`: http://www.python.org/dev/peps/pep-0257/

* Lines should not exceed 78 columns.

  You can enforce this in :program:`vim` by setting the ``textwidth`` option:

  .. code-block:: vim

        set textwidth=78

  If adhering to this limit makes the code less readable, you have one more
  character to go on, which means 78 is a soft limit, and 79 is the hard
  limit :)

* Import order

    * Python standard library (`import xxx`)
    * Python standard library ('from xxx import`)
    * Third party packages.
    * Other modules from the current package.

    or in case of code using Django:

    * Python standard library (`import xxx`)
    * Python standard library ('from xxx import`)
    * Third party packages.
    * Django packages.
    * Other modules from the current package.

    Within these sections the imports should be sorted by module name.

    Example:

    .. code-block:: python

        import threading
        import time

        from collections import deque
        from Queue import Queue, Empty

        from .datastructures import TokenBucket
        from .utils import timeutils
        from .utils.compat import all, izip_longest, chain_from_iterable

* Wildcard imports must not be used (`from xxx import *`).

* For distributions where Python 2.5 is the oldest support version
  additional rules apply:

    * Absolute imports must be enabled at the top of every module::

        from __future__ import absolute_import

    * If the module uses the with statement it must also enable that::

        from __future__ import with_statement

    * Every future import must be on its own line, as older Python 2.5
      releases did not support importing multiple features on the
      same future import line::

        # Good
        from __future__ import absolute_import
        from __future__ import with_statement

        # Bad
        from __future__ import absolute_import, with_statement

     (Note that this rule does not apply if the package does not include
     support for Python 2.5)


* Note that we use "new-style` relative imports when the distribution
  does not support Python versions below 2.5

.. code-block:: python

        from . import submodule

.. _release-procedure:

Release Procedure
=================

Updating the version number
---------------------------

The version number must be updated two places:

    * :file:`celery/__init__.py`
    * :file:`docs/include/introduction.txt`

After you have changed these files you must render
the :file:`README` files.  There is a script to convert sphinx syntax
to generic reStructured Text syntax, and the paver task `readme`
does this for you::

    $ paver readme

Now commit the changes::

    $ git commit -a -m "Bumps version to X.Y.Z"

and make a new version tag::

    $ git tag vX.Y.Z
    $ git push --tags

Releasing
---------

Commands to make a new public stable release::

    $ paver releaseok     # checks pep8, autodoc index and runs tests
    $ paver removepyc  # Remove .pyc files.
    $ git clean -xdn # Check that there's no left-over files in the repository.
    $ python2.5 setup.py sdist upload # Upload package to PyPI
    $ paver upload_pypi_docs
    $ paver ghdocs # Build and upload documentation to Github.

If this is a new release series then you also need to do the
following:

* Go to the Read The Docs management interface at:
    http://readthedocs.org/projects/celery/?fromdocs=celery

* Enter "Edit project"

    Change default branch to the branch of this series, e.g. ``2.4``
    for series 2.4.

* Also add the previous version under the "versions" tab.


Updating bundles
----------------

First you need to make sure the bundle entrypoints have been installed,
but either running `develop`, or `install`::

    $ python setup.py develop

Then make sure that you have your PyPI credentials stored in
:file:`~/.pypirc`, and execute the command::

    $ python setup.py upload_bundles

If you broke something and need to update new versions of the bundles,
then you can use ``upload_bundles_fix``.
