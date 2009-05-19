"""
Sphinx plugins for Django documentation.
"""

import docutils.nodes
import docutils.transforms
import sphinx
import sphinx.addnodes
import sphinx.directives
import sphinx.environment
import sphinx.roles
from docutils import nodes

def setup(app):
    app.add_crossref_type(
        directivename = "setting",
        rolename      = "setting",
        indextemplate = "pair: %s; setting",
    )
    app.add_crossref_type(
        directivename = "templatetag",
        rolename      = "ttag",
        indextemplate = "pair: %s; template tag"
    )
    app.add_crossref_type(
        directivename = "templatefilter",
        rolename      = "tfilter",
        indextemplate = "pair: %s; template filter"
    )
    app.add_crossref_type(
        directivename = "fieldlookup",
        rolename      = "lookup",
        indextemplate = "pair: %s, field lookup type",
    )
    app.add_description_unit(
        directivename = "django-admin",
        rolename      = "djadmin",
        indextemplate = "pair: %s; django-admin command",
        parse_node    = parse_django_admin_node,
    )
    app.add_description_unit(
        directivename = "django-admin-option",
        rolename      = "djadminopt",
        indextemplate = "pair: %s; django-admin command-line option",
        parse_node    = lambda env, sig, signode: sphinx.directives.parse_option_desc(signode, sig),
    )
    app.add_config_value('django_next_version', '0.0', True)
    app.add_directive('versionadded', parse_version_directive, 1, (1, 1, 1))
    app.add_directive('versionchanged', parse_version_directive, 1, (1, 1, 1))
    app.add_transform(SuppressBlockquotes)

def parse_version_directive(name, arguments, options, content, lineno,
                      content_offset, block_text, state, state_machine):
    env = state.document.settings.env
    is_nextversion = env.config.django_next_version == arguments[0]
    ret = []
    node = sphinx.addnodes.versionmodified()
    ret.append(node)
    if not is_nextversion:
        if len(arguments) == 1:
            linktext = 'Please, see the release notes <releases-%s>' % (arguments[0])
            xrefs = sphinx.roles.xfileref_role('ref', linktext, linktext, lineno, state)
            node.extend(xrefs[0])
        node['version'] = arguments[0]
    else:
        node['version'] = "Development version"
    node['type'] = name
    if len(arguments) == 2:
        inodes, messages = state.inline_text(arguments[1], lineno+1)
        node.extend(inodes)
        if content:
            state.nested_parse(content, content_offset, node)
        ret = ret + messages
    env.note_versionchange(node['type'], node['version'], node, lineno)
    return ret

                
class SuppressBlockquotes(docutils.transforms.Transform):
    """
    Remove the default blockquotes that encase indented list, tables, etc.
    """
    default_priority = 300
    
    suppress_blockquote_child_nodes = (
        docutils.nodes.bullet_list, 
        docutils.nodes.enumerated_list, 
        docutils.nodes.definition_list,
        docutils.nodes.literal_block, 
        docutils.nodes.doctest_block, 
        docutils.nodes.line_block, 
        docutils.nodes.table
    )
    
    def apply(self):
        for node in self.document.traverse(docutils.nodes.block_quote):
            if len(node.children) == 1 and isinstance(node.children[0], self.suppress_blockquote_child_nodes):
                node.replace_self(node.children[0])


def parse_django_admin_node(env, sig, signode):
    command = sig.split(' ')[0]
    env._django_curr_admin_command = command
    title = "django-admin.py %s" % sig
    signode += sphinx.addnodes.desc_name(title, title)
    return sig
