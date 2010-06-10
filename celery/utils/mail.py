from mailer import Message, Mailer

def mail_admins(subject, message, fail_silently=False):
    """Send a message to the admins in conf.ADMINS."""
    from celery import conf

    if not conf.ADMINS:
        return

    to = ", ".join(admin_email for _, admin_email in conf.ADMINS)
    username = conf.EMAIL_HOST_USER
    password = conf.EMAIL_HOST_PASSWORD

    message = Message(From=conf.SERVER_EMAIL, To=to,
                      Subject=subject, Body=message)

    try:
        mailer = Mailer(conf.EMAIL_HOST, conf.EMAIL_PORT)
        username and mailer.login(username, password)
        mailer.send(message)
    except Exception:
        if not fail_silently:
            raise
