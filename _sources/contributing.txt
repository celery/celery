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

    Within these sections imports should be sorted by name.

    Example:

    .. code-block:: python

        import threading
        import time

        from collections import deque
        from Queue import Queue, Empty

        from celery.datastructures import TokenBucket
        from celery.utils import timeutils
        from celery.utils.compat import all, izip_longest, chain_from_iterable

* Wildcard imports must not be used (`from xxx import *`).
