# -*- coding: utf-8 -*-

import sys
import os

this = os.path.dirname(os.path.abspath(__file__))

# If your extensions are in another directory, add it here. If the directory
# is relative to the documentation root, use os.path.abspath to make it
# absolute, like shown here.
sys.path.insert(0, os.path.join(this, os.pardir))
sys.path.append(os.path.join(this, '_ext'))
import celery

# General configuration
# ---------------------

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.coverage',
              'sphinx.ext.pngmath',
              'sphinx.ext.viewcode',
              'sphinx.ext.coverage',
              'sphinx.ext.intersphinx',
              'celery.contrib.sphinx',
              'githubsphinx',
              'celerydocs']


LINKCODE_URL = 'http://github.com/{proj}/tree/{branch}/{filename}.py'
GITHUB_PROJECT = 'celery/celery'
GITHUB_BRANCH = 'master'


def linkcode_resolve(domain, info):
    if domain != 'py' or not info['module']:
        return
    filename = info['module'].replace('.', '/')
    return LINKCODE_URL.format(
        proj=GITHUB_PROJECT,
        branch=GITHUB_BRANCH,
        filename=filename,
    )

html_show_sphinx = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ['.templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'Celery'
copyright = '2009-2014, Ask Solem & Contributors'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = '.'.join(map(str, celery.VERSION[0:2]))
# The full version, including alpha/beta/rc tags.
release = celery.__version__

exclude_trees = ['.build']

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

intersphinx_mapping = {
    'python': ('http://docs.python.org/dev', None),
    'kombu': ('http://kombu.readthedocs.org/en/master/', None),
    'djcelery': ('http://django-celery.readthedocs.org/en/master', None),
    'cyme': ('http://cyme.readthedocs.org/en/latest', None),
    'amqp': ('http://amqp.readthedocs.org/en/latest', None),
    'flower': ('http://flower.readthedocs.org/en/latest', None),
}

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'colorful'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['.static']

html_use_smartypants = True

add_module_names = True
highlight_language = 'python3'

# If false, no module index is generated.
html_use_modindex = True

# If false, no index is generated.
html_use_index = True

latex_documents = [
    ('index', 'Celery.tex', 'Celery Documentation',
     'Ask Solem & Contributors', 'manual'),
]

html_theme = 'celery'
html_theme_path = ['_theme']
html_sidebars = {
    'index': ['sidebarintro.html', 'sourcelink.html', 'searchbox.html'],
    '**': ['sidebarlogo.html', 'relations.html',
           'sourcelink.html', 'searchbox.html'],
}

# ## Issuetracker

github_project = 'celery/celery'

# -- Options for Epub output -------------------------------------------

# Bibliographic Dublin Core info.
epub_title = 'Celery Manual, Version {0}'.format(version)
epub_author = 'Ask Solem'
epub_publisher = 'Celery Project'
epub_copyright = '2009-2014'

# The language of the text. It defaults to the language option
# or en if the language is not set.
epub_language = 'en'

# The scheme of the identifier. Typical schemes are ISBN or URL.
epub_scheme = 'ISBN'

# The unique identifier of the text. This can be a ISBN number
# or the project homepage.
epub_identifier = 'celeryproject.org'

# A unique identification for the text.
epub_uid = 'Celery Manual, Version {0}'.format(version)

# ## HTML files that should be inserted before the pages created by sphinx.
# ## The format is a list of tuples containing the path and title.
# epub_pre_files = []

# ## HTML files shat should be inserted after the pages created by sphinx.
# ## The format is a list of tuples containing the path and title.
# epub_post_files = []

# A list of files that should not be packed into the epub file.
epub_exclude_files = ['search.html']


# The depth of the table of contents in toc.ncx.
epub_tocdepth = 3
