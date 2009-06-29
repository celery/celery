PEP8=pep8

pep8:
	(find . -name "*.py" | xargs pep8 | perl -nle'\
		print; $$a=1 if $$_}{exit($$a)')

ghdocs:
	contrib/doc2ghpages

autodoc:
	contrib/doc4allmods celery

bump:
	contrib/bump -c celery

coverage:
	(cd testproj; python manage.py test --figleaf)

test:
	(cd testproj; python manage.py test)

releaseok: pep8 autodoc test

removepyc:
	find . -name "*.pyc" | xargs rm

release: releaseok removepyc ghdocs

