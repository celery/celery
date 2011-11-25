.. _guide-security:

==========
 Security
==========

.. contents::
    :local:

Introduction
============

While Celery is written with security in mind, it should, like everything
else, be treated as an unsafe component.

Depending on your `Security Policy`_, there are
various steps you can take to make your Celery installation more secure.


.. _`Security Policy`: http://en.wikipedia.org/wiki/Security_policy


Areas of Concern
================

Broker
------

It is imperative that the broker is guarded from unwanted access.
The worker must not automatically trust the messages it receives
from the broker, especially if the broker is publicly accessible.

If possible, the first step should be to use a firewall to deny access
to the broker, so that only white-listed machines have access to the port
the broker is listening on.

While firewalls are usually effective, they don't help
if someone temporarily disables them,  leaving a statistically enough short
window of attack to be fairly safe, but then that same someone forgets
to re-enable it and it's not detected until months later.
This along with firewall misconfiguration
happens frequently in the real world, and with all the other possible
ways to get behind private networks, this is why you should never
trust that something behind a firewall is protected, and does not need
further security measures.

If your broker supports fine-grained access control, like RabbitMQ,
this is something you should look at enabling.  See for example
http://www.rabbitmq.com/access-control.html.

Client
------

In Celery, "client" refers to anything that sends
messages to the broker, e.g. web-servers that apply tasks.

Having your broker properly secured doesn't matter if arbitrary messages
can be sent through a client.

*[Need more text here]*

Worker
------

By default any task executed in the worker has the same access to the workers
memory space, file-system and devices as the privileges of the
current user.  That is except when using the multiprocessing pool, where
dedicated worker processes for tasks means it will have access to
any memory copied as a result of the :func:`fork` call (does not apply
under MS Windows), and access to memory contents written
by parent tasks in the same worker child process.

Limiting access to memory contents can be done by launching every task
in a subprocess (:func:`fork` + :func:`execve`).

Limiting file-system and device access can be accomplished by using
`chroot`_, `jail`_, `sandboxing`_, virtual machines or other
mechanisms as enabled by the platform or additional software.

Also remember that any task executed in the worker will have the
same network access as the machine it is running on, so if the worker is
located on an internal network adding firewall rules to the workers
outbound traffic is a good idea.

.. _`chroot`: http://en.wikipedia.org/wiki/Chroot
.. _`jail`: http://en.wikipedia.org/wiki/FreeBSD_jail
.. _`sandboxing`:
    http://en.wikipedia.org/wiki/Sandbox_(computer_security)

Serializers
===========

*[To be written]*

Message Signing
===============

*[To be written]*

Intrusion Detection
===================

The most important part when defending your systems against
intruders is being able to detect if the system has been compromised.

Logs
----

Logs are often the first place we go to to find evidence
of security breaches, but the logs are useless if they can be tampered with.

The best solution is to set up centralized logging with a dedicated logging
server with restricted access that all your servers can log to.
In addition to having all of the logs in a single place, if configured
correctly it can make it harder for intruders to tamper with your logs.

This should be fairly easy to setup using syslog (see also `syslog-ng`_ and
`rsyslog`_.).  Celery uses the :mod:`logging` library, and already has
built-in support for using syslog.

A tip for the paranoid is to send logs using UDP and cut the
transmit part of the logging servers network cable :-)

.. _`syslog-ng`: http://en.wikipedia.org/wiki/Syslog-ng
.. _`rsyslog`: http://www.rsyslog.com/

Tripwire
--------

`Tripwire`_ is a (now commercial) data integrity tool, with several
open source implementations, used to keep
cryptographic hashes of files in the file-system, so that administrators
can be alerted when they change.  This way when the damage is done and your
system has been compromised you can tell exactly what files the intruders
changed  (password files, logs, backdoors, rootkits and so on).
Often this is the only way you will be able to detect an intrusion.

Some of the open source implementations include:

* `OSSEC`_
* `Samhain`_
* `Open Source Tripwire`_
* `AIDE`_

Also, the `ZFS`_ file-system comes with built-in integrity checks
that can be used.

.. _`Tripwire`: http://tripwire.com/
.. _`OSSEC`: http://www.ossec.net/
.. _`Samhain`: http://la-samhna.de/samhain/index.html
.. _`AIDE`: http://aide.sourceforge.net/
.. _`Open Source Tripwire`: http://sourceforge.net/projects/tripwire/
.. _`ZFS`: http://en.wikipedia.org/wiki/ZFS
