from docutils import nodes

from sphinx.environment import NoUri
from sphinx.util.nodes import make_refnode

APPATTRS = {
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

ABBRS = {
    "Celery": "celery.app.Celery",
}

ABBR_EMPTY = {
    "exc": "celery.exceptions",
}
DEFAULT_EMPTY = "celery.app.Celery"


def typeify(S, type):
    if type in ("meth", "func"):
        return S + '()'
    return S


def shorten(S, newtarget, src_dict):
    if S.startswith('@-'):
        return S[2:]
    elif S.startswith('@'):
        if src_dict is APPATTRS:
            return '.'.join([pkg_of(newtarget), S[1:]])
        return S[1:]
    return S


def get_abbr(pre, rest, type):
    if pre:
        for d in APPATTRS, ABBRS:
            try:
                return d[pre], rest, d
            except KeyError:
                pass
        raise KeyError(pre)
    else:
        for d in APPATTRS, ABBRS:
            try:
                return d[rest], '', d
            except KeyError:
                pass
    return ABBR_EMPTY.get(type, DEFAULT_EMPTY), rest, ABBR_EMPTY



def resolve(S, type):
    is_appattr = False
    if S.startswith('@'):
        S = S.lstrip('@-')
        try:
            pre, rest = S.split('.', 1)
        except ValueError:
            pre, rest = '', S

        target, rest, src = get_abbr(pre, rest, type)
        return '.'.join([target, rest]) if rest else target, src
    return S, None


def pkg_of(module_fqdn):
    return module_fqdn.split('.', 1)[0]


def basename(module_fqdn):
    return module_fqdn.lstrip('@').rsplit('.', -1)[-1]


def modify_textnode(T, newtarget, node, src_dict, type):
    src = node.children[0].rawsource
    return nodes.Text(
        typeify(basename(T), type) if '~' in src
                                   else typeify(shorten(T, newtarget,
                                                        src_dict), type),
        src,
    )


def maybe_resolve_abbreviations(app, env, node, contnode):
    domainname = node.get('refdomain')
    target = node["reftarget"]
    type = node["reftype"]
    if target.startswith('@'):
        newtarget, src_dict = resolve(target, type)
        node["reftarget"] = newtarget
        # shorten text if '~' is not enabled.
        if len(contnode) and isinstance(contnode[0], nodes.Text):
                contnode[0] = modify_textnode(target, newtarget, node,
                                              src_dict, type)
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
