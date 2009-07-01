PEP8=pep8

pep8:
	(find . -name "*.py" | xargs pep8 | perl -nle'\
		print; $$a=1 if $$_}{exit($$a)')

cycomplex:
	find celery -type f -name "*.py" | xargs pygenie.py complexity

ghdocs:
	contrib/doc2ghpages

autodoc:
	contrib/doc4allmods celery

bump:
	contrib/bump -c celery

coverage2:
	[ -d testproj/temp ] || mkdir -p testproj/temp
	(cd testproj; python manage.py test --figleaf)

coverage:
	[ -d testproj/temp ] || mkdir -p testproj/temp
	(cd testproj; python manage.py test --coverage)

test:
	(cd testproj; python manage.py test)

testverbose:
	(cd testproj; python manage.py test --verbosity=2)

releaseok: pep8 autodoc test

removepyc:
	find . -name "*.pyc" | xargs rm

release: releaseok ghdocs removepyc

