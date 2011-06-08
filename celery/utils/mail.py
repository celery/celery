import sys
import smtplib

try:
    from email.mime.text import MIMEText
except ImportError:
    from email.MIMEText import MIMEText  # noqa

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
            timeout=2, use_ssl=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.use_ssl = use_ssl

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

        if self.user and self.password:
            client.login(self.user, self.password)

        client.sendmail(message.sender, message.to, str(message))
        client.quit()
