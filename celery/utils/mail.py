import smtplib

try:
    from email.mime.text import MIMEText
except ImportError:
    from email.MIMEText import MIMEText

class Message(object):

    def __init__(self, to=None, sender=None, subject=None, body=None,
            charset="us-ascii"):
        self.to = to
        self.sender = sender
        self.subject = subject
        self.body = body

        if not isinstance(self.to, (list, tuple)):
            self.to = [self.to]

    def __str__(self):
        msg = MIMEText(self.body, "plain", self.charset)
        msg["Subject"] = self.subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.to)
        return msg.as_string()


class Mailer(object):

    def __init__(self, host="localhost", port=0, user=None, password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def send(self, message):
        client = smtplib.SMTP(self.host, self.port)

        if self.user and self.password:
            server.login(self.user, self.password)

        client.sendmail(message.sender, message.to, str(message))
        client.quit()



def mail_admins(subject, message, fail_silently=False):
    """Send a message to the admins in conf.ADMINS."""
    from celery import conf

    if not conf.ADMINS:
        return

    to = [admin_email for _, admin_email in conf.ADMINS]
    message = Message(sender=conf.SERVER_EMAIL, to=to,
                      subject=subject, body=message)

    try:
        mailer = Mailer(conf.EMAIL_HOST, conf.EMAIL_PORT,
                        conf.EMAIL_HOST_USER,
                        conf.EMAIL_HOST_PASSWORD)
        mailer.send(message)
    except Exception:
        if not fail_silently:
            raise
