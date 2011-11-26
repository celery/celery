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

Celery uses `pickle` as default serializer. `pickle`_ is an insecure
serialization method and should be avoided in cases when clients are
untrusted or unauthenticated.

Celery has a special `auth` serializer which is intended for authenticating
the communication between Celery clients and workers. The `auth` serializer
uses public-key cryptography to check the authenticity of senders. See
`Message Signing`_ for information on how to enable the `auth` serializer.

.. _`pickle`: http://docs.python.org/library/pickle.html

.. _message-signing:

Message Signing
===============

Celery uses public-key cryptography to sign messages. Messages exchanged
between clients and workers are signed with private key and
verified with public certificate.

Celery uses `X.509`_ certificates and `pyOpenSSL`_ library for message signing.
Normally, the certificates should be signed by a Certificate Authority,
but they can be self-signed or signed by an untrusted third party.

The message signing is implemented in the `auth` serializer.
The `auth` serializer can be enabled with :setting:`CELERY_TASK_SERIALIZER`
configuration option. The `auth` serializer requires
:setting:`CELERY_SECURITY_KEY`, :setting:`CELERY_SECURITY_CERTIFICATE` and
:setting:`CELERY_SECURITY_CERT_STORE` configuration options to be provided.
They are used for locating private-keys and certificates.
After providing :setting:`CELERY_SECURITY_*` options it is necessary to call
:meth:`celery.security.setup_security` method. :meth:`celery.security.setup_security`
disables all insecure serializers.

.. code-block:: python

    # sample Celery auth configuration
    CELERY_SECURITY_KEY = "/etc/ssl/private/worker.key"
    CELERY_SECURITY_CERTIFICATE = "/etc/ssl/certs/worker.pem"
    CELERY_SECURITY_CERT_STORE = "/etc/ssl/certs/*.pem"
    from celery.security import setup_security
    setup_security()

.. note::

    The `auth` serializer doesn't encrypt the content of a message

.. setting:: CELERY_TASK_ERROR_WHITELIST

.. _`pyOpenSSL`: http://pypi.python.org/pypi/pyOpenSSL
.. _`X.509`: http://en.wikipedia.org/wiki/X.509

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
