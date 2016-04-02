"""Stolen from sphinxcontrib-issuetracker.

Had to modify this as the original will make one Github API request
per issue, which is not at all needed if we just want to link to issues.

"""
from __future__ import absolute_import, unicode_literals

import re
import sys

from collections import namedtuple

from docutils import nodes
from docutils.transforms import Transform
from sphinx.roles import XRefRole
from sphinx.addnodes import pending_xref

URL = 'https://github.com/{project}/issues/{issue_id}'

Issue = namedtuple('Issue', ('id', 'title', 'url'))

if sys.version_info[0] == 3:
    str_t = text_t = str
else:
    str_t = basestring
    text_t = unicode


class IssueRole(XRefRole):
    innernodeclass = nodes.inline


class Issues(Transform):
    default_priority = 999

    def apply(self):
        config = self.document.settings.env.config
        github_project = config.github_project
        issue_pattern = config.github_issue_pattern
        if isinstance(issue_pattern, str_t):
            issue_pattern = re.compile(issue_pattern)
        for node in self.document.traverse(nodes.Text):
            parent = node.parent
            if isinstance(parent, (nodes.literal, nodes.FixedTextElement)):
                continue
            text = text_t(node)
            new_nodes = []
            last_issue_ref_end = 0
            for match in issue_pattern.finditer(text):
                head = text[last_issue_ref_end:match.start()]
                if head:
                    new_nodes.append(nodes.Text(head))
                last_issue_ref_end = match.end()
                issuetext = match.group(0)
                issue_id = match.group(1)
                refnode = pending_xref()
                refnode['reftarget'] = issue_id
                refnode['reftype'] = 'issue'
                refnode['github_project'] = github_project
                reftitle = issuetext
                refnode.append(nodes.inline(
                    issuetext, reftitle, classes=['xref', 'issue']))
                new_nodes.append(refnode)
            if not new_nodes:
                continue
            tail = text[last_issue_ref_end:]
            if tail:
                new_nodes.append(nodes.Text(tail))
            parent.replace(node, new_nodes)


def make_issue_reference(issue, content_node):
    reference = nodes.reference()
    reference['refuri'] = issue.url
    if issue.title:
        reference['reftitle'] = issue.title
    reference.append(content_node)
    return reference


def resolve_issue_reference(app, env, node, contnode):
    if node['reftype'] != 'issue':
        return
    issue_id = node['reftarget']
    project = node['github_project']

    issue = Issue(issue_id, None, URL.format(project=project,
                                             issue_id=issue_id))
    conttext = text_t(contnode[0])
    formatted_conttext = nodes.Text(conttext.format(issue=issue))
    formatted_contnode = nodes.inline(conttext, formatted_conttext,
                                      classes=contnode['classes'])
    return make_issue_reference(issue, formatted_contnode)


def init_transformer(app):
    app.add_transform(Issues)


def setup(app):
    app.require_sphinx('1.0')
    app.add_role('issue', IssueRole())

    app.add_config_value('github_project', None, 'env')
    app.add_config_value('github_issue_pattern',
                         re.compile(r'[Ii]ssue #(\d+)'), 'env')

    app.connect(str('builder-inited'), init_transformer)
    app.connect(str('missing-reference'), resolve_issue_reference)
