# -*- coding: utf-8 -*-

import sys
import os

# eventlet/gevent should not monkey patch anything.
os.environ["GEVENT_NOPATCH"] = "yes"
os.environ["EVENTLET_NOPATCH"] = "yes"
os.environ["CELERY_LOADER"] = "default"

this = os.path.dirname(os.path.abspath(__file__))

# If your extensions are in another directory, add it here. If the directory
# is relative to the documentation root, use os.path.abspath to make it
# absolute, like shown here.
sys.path.append(os.path.join(os.pardir, "tests"))
sys.path.append(os.path.join(this, "_ext"))
import celery


# use app loader
from celery import Celery
app = Celery(set_as_current=True)
app.conf.update(BROKER_TRANSPORT="memory",
                CELERY_RESULT_BACKEND="cache",
                CELERY_CACHE_BACKEND="memory",
                CELERYD_HIJACK_ROOT_LOGGER=False,
                CELERYD_LOG_COLOR=False)

# General configuration
# ---------------------

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.coverage',
              'sphinx.ext.pngmath',
              'sphinx.ext.intersphinx',
              'sphinxcontrib.issuetracker',
              'celerydocs']

html_show_sphinx = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ['.templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = u'Celery'
copyright = u'2009-2012, Ask Solem & Contributors'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = ".".join(map(str, celery.VERSION[0:2]))
# The full version, including alpha/beta/rc tags.
release = celery.__version__

exclude_trees = ['.build']

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

intersphinx_mapping = {
        "http://docs.python.org/dev": None,
        "http://kombu.readthedocs.org/en/latest/": None,
        "http://django-celery.readthedocs.org/en/latest": None,
}

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'trac'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['.static']

html_use_smartypants = True

# If false, no module index is generated.
html_use_modindex = True

# If false, no index is generated.
html_use_index = True

latex_documents = [
  ('index', 'Celery.tex', ur'Celery Documentation',
   ur'Ask Solem & Contributors', 'manual'),
]

html_theme = "celery"
html_theme_path = ["_theme"]
html_sidebars = {
    'index': ['sidebarintro.html', 'sourcelink.html', 'searchbox.html'],
    '**': ['sidebarlogo.html', 'relations.html',
           'sourcelink.html', 'searchbox.html'],
}

### Issuetracker

if False:
    issuetracker = "github"
    issuetracker_project = "ask/celery"
    issuetracker_issue_pattern = r'[Ii]ssue #(\d+)'
