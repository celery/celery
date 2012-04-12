from docutils import nodes

from sphinx.environment import NoUri
from sphinx.util.nodes import make_refnode

ABBR = {
    "": "celery.app.base.Celery",
    "amqp": "celery.app.amqp.AMQP",
    "backend": "celery.backends.base.BaseBackend",
    "control": "celery.app.control.Control",
    "events": "celery.events.Events",
    "loader": "celery.app.loaders.base.BaseLoader",
    "log": "celery.app.log.Logging",
    "pool": "kombu.connection.ConnectionPool",
    "tasks": "celery.app.registry.Registry",

    "AsyncResult": "celery.result.AsyncResult",
    "TaskSetResult": "celery.result.TaskSetResult",
    "Worker": "celery.apps.worker.Worker",
    "WorkController": "celery.worker.WorkController",
    "Beat": "celery.apps.beat.Beat",
    "Task": "celery.app.task.BaseTask",
}

ABBR_EMPTY = {
    "exc": "celery.exceptions",
}
DEFAULT_EMPTY = "celery.app.base.Celery"


def shorten(S, base):
    if S.startswith('@-'):
        return S[2:]
    elif S.startswith('@'):
        print("S: %r BASE: %r" % (S, base))
        return '.'.join([base, S[1:]])
    return S


def resolve(S, type):
    X = S
    if S.startswith('@'):
        S = S.lstrip('@-')
        try:
            pre, rest = S.split('.', 1)
        except ValueError:
            pre, rest = '', S
        return '.'.join([ABBR[pre] if pre
                                   else ABBR_EMPTY.get(type, DEFAULT_EMPTY),
                         rest])


def pkg_of(module_fqdn):
    return module_fqdn.split('.', 1)[0]


def modify_textnode(T, newtarget, node):
    src = node.children[0].rawsource
    return nodes.Text(
        T.lstrip('@') if '~' in src else shorten(T, pkg_of(newtarget)),
        src
    )


def maybe_resolve_abbreviations(app, env, node, contnode):
    domainname = node.get('refdomain')
    target = node["reftarget"]
    type = node["reftype"]
    if target.startswith('@'):
        newtarget = node["reftarget"] = resolve(target, type)
        # shorten text if '~' is not enabled.
        if len(contnode) and isinstance(contnode[0], nodes.Text):
                contnode[0] = modify_textnode(target, newtarget, node)
        if domainname:
            try:
                domain = env.domains[node.get("refdomain")]
            except KeyError:
                raise NoUri
            return domain.resolve_xref(env, node["refdoc"], app.builder,
                                       type, newtarget,
                                       node, contnode)


def setup(app):
    app.connect('missing-reference', maybe_resolve_abbreviations)

    app.add_crossref_type(
        directivename="setting",
        rolename="setting",
        indextemplate="pair: %s; setting",
    )
    app.add_crossref_type(
        directivename="sig",
        rolename="sig",
        indextemplate="pair: %s; sig",
    )
    app.add_crossref_type(
        directivename="state",
        rolename="state",
        indextemplate="pair: %s; state",
    )
    app.add_crossref_type(
        directivename="control",
        rolename="control",
        indextemplate="pair: %s; control",
    )
    app.add_crossref_type(
        directivename="signal",
        rolename="signal",
        indextemplate="pair: %s; signal",
    )
