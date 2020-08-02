import typing

from docutils import nodes
from sphinx.errors import NoUri

APPATTRS = {
    'amqp': 'celery.app.amqp.AMQP',
    'backend': 'celery.backends.base.BaseBackend',
    'conf': 'celery.app.utils.Settings',
    'control': 'celery.app.control.Control',
    'events': 'celery.events.Events',
    'loader': 'celery.app.loaders.base.BaseLoader',
    'log': 'celery.app.log.Logging',
    'pool': 'kombu.connection.ConnectionPool',
    'tasks': 'celery.app.registry.Registry',

    'AsyncResult': 'celery.result.AsyncResult',
    'ResultSet': 'celery.result.ResultSet',
    'GroupResult': 'celery.result.GroupResult',
    'Worker': 'celery.apps.worker.Worker',
    'WorkController': 'celery.worker.WorkController',
    'Beat': 'celery.apps.beat.Beat',
    'Task': 'celery.app.task.Task',
    'signature': 'celery.canvas.Signature',
}

APPDIRECT = {
    'on_configure', 'on_after_configure', 'on_after_finalize',
    'set_current', 'set_default', 'close', 'on_init', 'start',
    'worker_main', 'task', 'gen_task_name', 'finalize',
    'add_defaults', 'config_from_object', 'config_from_envvar',
    'config_from_cmdline', 'setup_security', 'autodiscover_tasks',
    'send_task', 'connection', 'connection_or_acquire',
    'producer_or_acquire', 'prepare_config', 'now',
    'select_queues', 'either', 'bugreport', 'create_task_cls',
    'subclass_with_self', 'annotations', 'current_task', 'oid',
    'timezone', '__reduce_keys__', 'fixups', 'finalized', 'configured',
    'add_periodic_task',
    'autofinalize', 'steps', 'user_options', 'main', 'clock',
}

APPATTRS.update({x: f'celery.Celery.{x}' for x in APPDIRECT})

ABBRS = {
    'Celery': 'celery.Celery',
}

ABBR_EMPTY = {
    'exc': 'celery.exceptions',
}
DEFAULT_EMPTY = 'celery.Celery'


def typeify(S, type):
    if type in ('meth', 'func'):
        return S + '()'
    return S


def shorten(S, newtarget, src_dict):
    if S.startswith('@-'):
        return S[2:]
    elif S.startswith('@'):
        if src_dict is APPATTRS:
            return '.'.join(['app', S[1:]])
        return S[1:]
    return S


def get_abbr(pre, rest, type, orig=None):
    if pre:
        for d in APPATTRS, ABBRS:
            try:
                return d[pre], rest, d
            except KeyError:
                pass
        raise KeyError('Unknown abbreviation: {} ({})'.format(
            '.'.join([pre, rest]) if orig is None else orig, type,
        ))
    else:
        for d in APPATTRS, ABBRS:
            try:
                return d[rest], '', d
            except KeyError:
                pass
    return ABBR_EMPTY.get(type, DEFAULT_EMPTY), rest, ABBR_EMPTY


def resolve(S, type):
    if '.' not in S:
        try:
            getattr(typing, S)
        except AttributeError:
            pass
        else:
            return f'typing.{S}', None
    orig = S
    if S.startswith('@'):
        S = S.lstrip('@-')
        try:
            pre, rest = S.split('.', 1)
        except ValueError:
            pre, rest = '', S

        target, rest, src = get_abbr(pre, rest, type, orig)
        return '.'.join([target, rest]) if rest else target, src
    return S, None


def pkg_of(module_fqdn):
    return module_fqdn.split('.', 1)[0]


def basename(module_fqdn):
    return module_fqdn.lstrip('@').rsplit('.', -1)[-1]


def modify_textnode(T, newtarget, node, src_dict, type):
    src = node.children[0].rawsource
    return nodes.Text(
        (typeify(basename(T), type) if '~' in src
         else typeify(shorten(T, newtarget, src_dict), type)),
        src,
    )


def maybe_resolve_abbreviations(app, env, node, contnode):
    domainname = node.get('refdomain')
    target = node['reftarget']
    type = node['reftype']
    if target.startswith('@'):
        newtarget, src_dict = resolve(target, type)
        node['reftarget'] = newtarget
        # shorten text if '~' is not enabled.
        if len(contnode) and isinstance(contnode[0], nodes.Text):
            contnode[0] = modify_textnode(target, newtarget, node,
                                          src_dict, type)
        if domainname:
            try:
                domain = env.domains[node.get('refdomain')]
            except KeyError:
                raise NoUri
            try:
                return domain.resolve_xref(env, node['refdoc'], app.builder,
                                           type, newtarget,
                                           node, contnode)
            except KeyError:
                raise NoUri


def setup(app):
    app.connect(
        'missing-reference',
        maybe_resolve_abbreviations,
    )

    app.add_crossref_type(
        directivename='sig',
        rolename='sig',
        indextemplate='pair: %s; sig',
    )
    app.add_crossref_type(
        directivename='state',
        rolename='state',
        indextemplate='pair: %s; state',
    )
    app.add_crossref_type(
        directivename='control',
        rolename='control',
        indextemplate='pair: %s; control',
    )
    app.add_crossref_type(
        directivename='event',
        rolename='event',
        indextemplate='pair: %s; event',
    )

    return {
        'parallel_read_safe': True
    }
