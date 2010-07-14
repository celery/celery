from celery import log
from celery.messaging import ControlReplyPublisher, with_connection
from celery.utils import kwdict
from celery.worker.control.registry import Panel
from celery.worker.control import builtins


class ControlDispatch(object):
    """Execute worker control panel commands."""
    Panel = Panel
    ReplyPublisher = ControlReplyPublisher

    def __init__(self, logger=None, hostname=None, listener=None):
        self.logger = logger or log.get_default_logger()
        self.hostname = hostname
        self.listener = listener
        self.panel = self.Panel(self.logger, self.listener, self.hostname)

    @with_connection
    def reply(self, data, exchange, routing_key, connection=None,
            connect_timeout=None):
        crq = self.ReplyPublisher(connection, exchange=exchange)
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
            try:
                reply = control(self.panel, **kwdict(kwargs))
            except Exception, exc:
                self.logger.error(
                        "Error running control command %s kwargs=%s: %s" % (
                            command, kwargs, exc))
                reply = {"error": str(exc)}
            if reply_to:
                self.reply({self.hostname: reply},
                           exchange=reply_to["exchange"],
                           routing_key=reply_to["routing_key"])
            return reply
