PEP8=pep8

pep8:
	find . -name "*.py" | xargs pep8

ghdocs:
	contrib/doc2ghpages

autodoc:
	contrib/doc4allmods celery

bump:
	contrib/bump -c celery
