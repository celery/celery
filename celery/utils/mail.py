from mailer import Message, Mailer

from celery.loaders import load_settings


def mail_admins(subject, message, fail_silently=False):
    """Send a message to the admins in settings.ADMINS."""
    settings = load_settings()
    if not settings.ADMINS:
        return
    to = ", ".join(admin_email for _, admin_email in settings.ADMINS)
    username = settings.EMAIL_HOST_USER
    password = settings.EMAIL_HOST_PASSWORD

    message = Message(From=settings.SERVER_EMAIL, To=to,
                      Subject=subject, Message=message)

    try:
        mailer = Mailer(settings.EMAIL_HOST, settings.EMAIL_PORT)
        username and mailer.login(username, password)
        mailer.send(message)
    except Exception:
        if not fail_silently:
            raise
