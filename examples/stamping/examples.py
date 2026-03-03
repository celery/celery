from tasks import identity, identity_task
from visitors import FullVisitor, MonitoringIdStampingVisitor

from celery import chain, group


def run_example1():
    s1 = chain(identity_task.si("foo11"), identity_task.si("foo12"))
    s1.link(identity_task.si("link_foo1"))
    s1.link_error(identity_task.si("link_error_foo1"))

    s2 = chain(identity_task.si("foo21"), identity_task.si("foo22"))
    s2.link(identity_task.si("link_foo2"))
    s2.link_error(identity_task.si("link_error_foo2"))

    canvas = group([s1, s2])
    canvas.stamp(MonitoringIdStampingVisitor())
    canvas.delay()


def run_example2():
    sig1 = identity_task.si("sig1")
    sig1.link(identity_task.si("sig1_link"))
    sig2 = identity_task.si("sig2")
    sig2.link(identity_task.si("sig2_link"))
    s1 = chain(sig1, sig2)
    s1.link(identity_task.si("chain_link"))
    s1.stamp(FullVisitor())
    s1.stamp(MonitoringIdStampingVisitor())
    s1.delay()


def run_example3():
    sig1 = identity_task.si("sig1")
    sig1_link = identity_task.si("sig1_link")
    sig1.link(sig1_link)
    sig1_link.stamp(FullVisitor())
    sig1_link.stamp(MonitoringIdStampingVisitor())
    sig1.stamp(MonitoringIdStampingVisitor(), append_stamps=True)
    sig1.delay()


def run_example_with_replace():
    sig1 = identity.si("sig1")
    sig1.link(identity_task.si("sig1_link"))
    sig1.delay()
