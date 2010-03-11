import math

from celery import conf
from celery.messaging import establish_connection

ROUTE_FORMAT = """
. %(name)s -> exchange:%(exchange)s (%(exchange_type)s) \
binding:%(binding_key)s
"""
BROKER_FORMAT = "%(carrot_backend)s://%(userid)s@%(host)s%(port)s%(vhost)s"

TIME_UNITS = (("day", 60 * 60 * 24, lambda n: int(math.ceil(n))),
              ("hour", 60 * 60, lambda n: int(math.ceil(n))),
              ("minute", 60, lambda n: int(math.ceil(n))),
              ("second", 1, lambda n: "%.2f" % n))


def humanize_seconds(secs, prefix=""):
    """Show seconds in human form, e.g. 60 is "1 minute", 7200 is "2
    hours"."""
    for unit, divider, formatter in TIME_UNITS:
        if secs >= divider:
            w = secs / divider
            punit = w > 1 and unit+"s" or unit
            return "%s%s %s" % (prefix, formatter(w), punit)
    return "now"


def textindent(t, indent=0):
    """Indent text."""
    return "\n".join(" " * indent + p for p in t.split("\n"))


def format_routing_table(table=None, indent=0):
    """Format routing table into string for log dumps."""
    table = table or conf.routing_table
    format = lambda **route: ROUTE_FORMAT.strip() % route
    routes = "\n".join(format(name=name, **route)
                            for name, route in table.items())
    return textindent(routes, indent=indent)

def get_broker_info():
    broker_connection = establish_connection()

    carrot_backend = broker_connection.backend_cls
    if carrot_backend and not isinstance(carrot_backend, str):
        carrot_backend = carrot_backend.__name__
    carrot_backend = carrot_backend or "amqp"

    port = broker_connection.port or \
                broker_connection.get_backend_cls().default_port
    port = port and ":%s" % port or ""

    vhost = broker_connection.virtual_host
    if not vhost.startswith("/"):
        vhost = "/" + vhost

    return {"carrot_backend": carrot_backend,
            "userid": broker_connection.userid,
            "host": broker_connection.hostname,
            "port": port,
            "vhost": vhost}

def format_broker_info(info=None):
    """Get message broker connection info string for log dumps."""
    return BROKER_FORMAT % get_broker_info()
