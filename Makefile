PYTHON=python
SPHINX_DIR="docs/"
SPHINX_BUILDDIR="${SPHINX_DIR}/.build"
README="README.rst"
CONTRIBUTING="CONTRIBUTING.rst"
CONFIGREF_SRC="docs/configuration.rst"
README_SRC="docs/templates/readme.txt"
CONTRIBUTING_SRC="docs/contributing.rst"
SPHINX2RST="extra/release/sphinx-to-rst.py"
WORKER_GRAPH_FULL="docs/images/worker_graph_full.png"

SPHINX_HTMLDIR = "${SPHINX_BUILDDIR}/html"

html:
	(cd "$(SPHINX_DIR)"; make html)
	mv "$(SPHINX_HTMLDIR)" Documentation

docsclean:
	-rm -rf "$(SPHINX_BUILDDIR)"

htmlclean:
	-rm -rf "$(SPHINX)"

apicheck:
	extra/release/doc4allmods celery

indexcheck:
	extra/release/verify-reference-index.sh

configcheck:
	PYTHONPATH=. $(PYTHON) extra/release/verify_config_reference.py $(CONFIGREF_SRC)

flakecheck:
	flake8 celery

flakediag:
	-$(MAKE) flakecheck

flakepluscheck:
	flakeplus celery --2.6

flakeplusdiag:
	-$(MAKE) flakepluscheck

flakes: flakediag flakeplusdiag

readmeclean:
	-rm -f $(README)

readmecheck:
	iconv -f ascii -t ascii $(README) >/dev/null

$(README):
	$(PYTHON) $(SPHINX2RST) $(README_SRC) --ascii > $@

readme: readmeclean $(README) readmecheck

contributingclean:
	-rm -f CONTRIBUTING.rst

$(CONTRIBUTING):
	$(PYTHON) $(SPHINX2RST) $(CONTRIBUTING_SRC) > $@

contributing: contributingclean $(CONTRIBUTING)

test:
	nosetests -xv celery.tests

cov:
	nosetests -xv celery.tests --with-coverage --cover-html --cover-branch

removepyc:
	-find . -type f -a \( -name "*.pyc" -o -name "*$$py.class" \) | xargs rm
	-find . -type d -name "__pycache__" | xargs rm -r

$(WORKER_GRAPH_FULL):
	$(PYTHON) -m celery graph bootsteps | dot -Tpng -o $@

graphclean:
	-rm -f $(WORKER_GRAPH_FULL)

graph: graphclean $(WORKER_GRAPH_FULL)

gitclean:
	git clean -xdn

gitcleanforce:
	git clean -xdf

distcheck: flakecheck apicheck indexcheck configcheck readmecheck test gitclean

authorcheck:
	git shortlog -se | cut -f2 | extra/release/attribution.py

dist: readme contributing docsclean gitcleanforce removepyc
