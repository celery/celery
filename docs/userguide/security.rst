.. _guide-security:

==========
 Security
==========

.. contents::
    :local:

Introduction
============

While Celery is written with security in mind, it should be treated as an
unsafe component.

Depending on your `Security Policy`_, there are
various steps you can take to make your Celery installation more secure.


.. _`Security Policy`: http://en.wikipedia.org/wiki/Security_policy


Areas of Concern
================

Broker
------

It is imperative that the broker is guarded from unwanted access, especially
if it is publically accesible.
By default, workers trust that the data they get from the broker has not
been tampered with. See `Message Signing`_ for information on how to make
the broker connection more trusthworthy.

The first line of defence should be to put a firewall in front of the broker,
allowing only white-listed machines to access it.

Keep in mind that both firewall misconfiguration, and temproraily disabling
the firewall, is common in the real world. Solid security policy includes
monitoring of firewall equipment to detect if they have been disabled, be it 
accidentally or on purpose. 

In other words, one should not blindly trust the firewall either.

If your broker supports fine-grained access control, like RabbitMQ,
this is something you should look at enabling. See for example
http://www.rabbitmq.com/access-control.html.

Client
------

In Celery, "client" refers to anything that sends messages to the 
broker, e.g. web-servers that apply tasks.

Having the broker properly secured doesn't matter if arbitrary messages
can be sent through a client.

*[Need more text here]*

Worker
------

The default permissions of tasks running inside a worker are the same ones as
the privileges of the worker itself. This applies to resources such as 
memory, file-systems and devices.

An exception to this rule is when using the multiprocessing based task pool,
which is currently the default. In this case, the task will have access to
any memory copied as a result of the :func:`fork` call (does not apply
under MS Windows), and access to memory contents written
by parent tasks in the same worker child process.

Limiting access to memory contents can be done by launching every task
in a subprocess (:func:`fork` + :func:`execve`).

Limiting file-system and device access can be accomplished by using
`chroot`_, `jail`_, `sandboxing`_, virtual machines or other
mechanisms as enabled by the platform or additional software.

Note also that any task executed in the worker will have the
same network access as the machine on which it's running. If the worker
is located on an internal network it's recommended to add firewall rules for
outbound traffic.

.. _`chroot`: http://en.wikipedia.org/wiki/Chroot
.. _`jail`: http://en.wikipedia.org/wiki/FreeBSD_jail
.. _`sandboxing`:
    http://en.wikipedia.org/wiki/Sandbox_(computer_security)

Serializers
===========

The default `pickle` serializer is convenient because it supports
arbitrary Python objects, whereas other serializers only
work with a restricted set of types.

But for the same reasons the `pickle` serializer is inherently insecure[*]_,
and should be avoided whenever clients are untrusted or
unauthenticated.

.. [*] http://nadiana.com/python-pickle-insecure

Celery comes with a special `auth` serializer that validates
communication between Celery clients and workers, making sure
that messages originates from trusted sources.
Using `Public-key cryptography` the `auth` serializer can verify the
authenticity of senders, to enable this read :ref:`message-signing`
for more information.

.. _`pickle`: http://docs.python.org/library/pickle.html
.. _`Public-key cryptography`:
    http://en.wikipedia.org/wiki/Public-key_cryptography

.. _message-signing:

Message Signing
===============

Celery can use the `pyOpenSSL`_ library to sign message using
`Public-key cryptography`, where
messages sent by clients are signed using a private key
and then later verified by the worker using a public certificate.

Optimally certificates should be signed by an official
`Certificate Authority`_, but they can also be self-signed.

To enable this you should configure the :setting:`CELERY_TASK_SERIALIZER`
setting to use the `auth` serializer.
Also required is configuring the
paths used to locate private keys and certificates on the file-system:
the :setting:`CELERY_SECURITY_KEY`,
:setting:`CELERY_SECURITY_CERTIFICATE` and :setting:`CELERY_SECURITY_CERT_STORE`
settings respectively.
With these configured it is also necessary to call the
:func:`celery.security.setup_security` function.  Note that this will also
disable all insucure serializers so that the worker won't accept
messages with untrusted content types.

This is an example configuration using the `auth` serializer,
with the private key and certificate files located in :`/etc/ssl`.

.. code-block:: python

    CELERY_SECURITY_KEY = "/etc/ssl/private/worker.key"
    CELERY_SECURITY_CERTIFICATE = "/etc/ssl/certs/worker.pem"
    CELERY_SECURITY_CERT_STORE = "/etc/ssl/certs/\*.pem"
    from celery.security import setup_security
    setup_security()

.. note::

    While relative paths are not disallowed, using absolute paths
    is recommended for these files.

    Also note that the `auth` serializer won't encrypt the contents of
    a message, so if needed this will have to be enabled separately.

.. _`pyOpenSSL`: http://pypi.python.org/pypi/pyOpenSSL
.. _`X.509`: http://en.wikipedia.org/wiki/X.509
.. _`Certificate Authority`:
    http://en.wikipedia.org/wiki/Certificate_authority

Intrusion Detection
===================

The most important part when defending your systems against
intruders is being able to detect if the system has been compromised.

Logs
----

Logs are usually the first place to look for evidence
of security breaches, but they are useless if they can be tampered with.

A good solution is to set up centralized logging with a dedicated logging
server. Acess to it should be restricted.
In addition to having all of the logs in a single place, if configured
correctly, it can make it harder for intruders to tamper with your logs.

This should be fairly easy to setup using syslog (see also `syslog-ng`_ and
`rsyslog`_.).  Celery uses the :mod:`logging` library, and already has
support for using syslog.

A tip for the paranoid is to send logs using UDP and cut the
transmit part of the logging servers network cable :-)

.. _`syslog-ng`: http://en.wikipedia.org/wiki/Syslog-ng
.. _`rsyslog`: http://www.rsyslog.com/

Tripwire
--------

`Tripwire`_ is a (now commercial) data integrity tool, with several
open source implementations, used to keep
cryptographic hashes of files in the file-system, so that administrators
can be alerted when they change. This way when the damage is done and your
system has been compromised you can tell exactly what files intruders
have changed  (password files, logs, backdoors, rootkits and so on).
Often this is the only way you will be able to detect an intrusion.

Some open source implementations include:

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
