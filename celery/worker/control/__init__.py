from celery import log
from celery.worker.control.registry import Panel
from celery.worker.control import builtins
from celery.messaging import ControlReplyPublisher, with_connection


class ControlDispatch(object):
    """Execute worker control panel commands."""
    panel_cls = Panel

    def __init__(self, logger=None, hostname=None, listener=None):
        self.logger = logger or log.get_default_logger()
        self.hostname = hostname
        self.listener = listener
        self.panel = self.panel_cls(self.logger, self.listener, self.hostname)

    @with_connection
    def reply(self, data, exchange, routing_key, connection=None,
            connect_timeout=None):
        crq = ControlReplyPublisher(connection, exchange=exchange)
        try:
            crq.send(data, routing_key=routing_key)
        finally:
            crq.close()

    def dispatch_from_message(self, message):
        """Dispatch by using message data received by the broker.

        Example:

            >>> def receive_message(message_data, message):
            ...     control = message_data.get("control")
            ...     if control:
            ...         ControlDispatch().dispatch_from_message(control)

        """
        message = dict(message) # don't modify callers message.
        command = message.pop("command")
        destination = message.pop("destination", None)
        reply_to = message.pop("reply_to", None)
        if not destination or self.hostname in destination:
            return self.execute(command, message, reply_to=reply_to)

    def execute(self, command, kwargs=None, reply_to=None):
        """Execute control command by name and keyword arguments.

        :param command: Name of the command to execute.
        :param kwargs: Keyword arguments.

        """
        kwargs = kwargs or {}
        control = None
        try:
            control = self.panel[command]
        except KeyError:
            self.logger.error("No such control command: %s" % command)
        else:
            # need to make sure keyword arguments are not in unicode
            # this should be fixed in newer Python's
            # (see: http://bugs.python.org/issue4978)
            kwargs = dict((k.encode("utf8"), v)
                            for k, v in kwargs.iteritems())
            reply = control(self.panel, **kwargs)
            if reply_to:
                self.reply({self.hostname: reply},
                           exchange=reply_to["exchange"],
                           routing_key=reply_to["routing_key"])
            return reply
