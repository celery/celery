# -*- coding: utf-8 -*-
"""
    celery.utils.mail
    ~~~~~~~~~~~~~~~~~

    How task error emails are formatted and sent.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys
import smtplib

try:
    from email.mime.text import MIMEText
except ImportError:
    from email.MIMEText import MIMEText  # noqa

from celery.utils import get_symbol_by_name

supports_timeout = sys.version_info >= (2, 6)


class SendmailWarning(UserWarning):
    """Problem happened while sending the email message."""


class Message(object):

    def __init__(self, to=None, sender=None, subject=None, body=None,
            charset="us-ascii"):
        self.to = to
        self.sender = sender
        self.subject = subject
        self.body = body
        self.charset = charset

        if not isinstance(self.to, (list, tuple)):
            self.to = [self.to]

    def __repr__(self):
        return "<Email: To:%r Subject:%r>" % (self.to, self.subject)

    def __str__(self):
        msg = MIMEText(self.body, "plain", self.charset)
        msg["Subject"] = self.subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.to)
        return msg.as_string()


class Mailer(object):

    def __init__(self, host="localhost", port=0, user=None, password=None,
            timeout=2, use_ssl=False, use_tls=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.use_ssl = use_ssl
        self.use_tls = use_tls

    def send(self, message):
        if supports_timeout:
            self._send(message, timeout=self.timeout)
        else:
            import socket
            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(self.timeout)
            try:
                self._send(message)
            finally:
                socket.setdefaulttimeout(old_timeout)

    def _send(self, message, **kwargs):
        if (self.use_ssl):
            client = smtplib.SMTP_SSL(self.host, self.port, **kwargs)
        else:
            client = smtplib.SMTP(self.host, self.port, **kwargs)

        if self.use_tls:
            client.ehlo()
            client.starttls()
            client.ehlo()

        if self.user and self.password:
            client.login(self.user, self.password)

        client.sendmail(message.sender, message.to, str(message))
        client.quit()


class ErrorMail(object):
    """Defines how and when task error e-mails should be sent.

    :param task: The task instance that raised the error.

    :attr:`subject` and :attr:`body` are format strings which
    are passed a context containing the following keys:

    * name

        Name of the task.

    * id

        UUID of the task.

    * exc

        String representation of the exception.

    * args

        Positional arguments.

    * kwargs

        Keyword arguments.

    * traceback

        String representation of the traceback.

    * hostname

        Worker hostname.

    """

    # pep8.py borks on a inline signature separator and
    # says "trailing whitespace" ;)
    EMAIL_SIGNATURE_SEP = "-- "

    #: Format string used to generate error email subjects.
    subject = """\
        [celery@%(hostname)s] Error: Task %(name)s (%(id)s): %(exc)s
    """

    #: Format string used to generate error email content.
    body = """
Task %%(name)s with id %%(id)s raised exception:\n%%(exc)r


Task was called with args: %%(args)s kwargs: %%(kwargs)s.

The contents of the full traceback was:

%%(traceback)s

%(EMAIL_SIGNATURE_SEP)s
Just to let you know,
celeryd at %%(hostname)s.
""" % {"EMAIL_SIGNATURE_SEP": EMAIL_SIGNATURE_SEP}

    error_whitelist = None

    def __init__(self, task, **kwargs):
        self.task = task
        self.email_subject = kwargs.get("subject", self.subject)
        self.email_body = kwargs.get("body", self.body)
        self.error_whitelist = getattr(task, "error_whitelist")

    def should_send(self, context, exc):
        """Returns true or false depending on if a task error mail
        should be sent for this type of error."""
        allow_classes = tuple(map(get_symbol_by_name,  self.error_whitelist))
        return not self.error_whitelist or isinstance(exc, allow_classes)

    def format_subject(self, context):
        return self.subject.strip() % context

    def format_body(self, context):
        return self.body.strip() % context

    def send(self, context, exc, fail_silently=True):
        if self.should_send(context, exc):
            self.task.app.mail_admins(self.format_subject(context),
                                      self.format_body(context),
                                      fail_silently=fail_silently)
