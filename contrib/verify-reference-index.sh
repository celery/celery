#!/bin/bash
modules=$(grep "celery." docs/reference/index.rst | \
            perl -ple's/^\s*|\s*$//g;s{\.}{/}g;')
retval=0
for module in $modules; do
    if [ ! -f "$module.py" ]; then
        if [ ! -f "$module/__init__.py" ]; then
            echo "Outdated reference: $module"
            retval=1
        fi
    fi
done

exit $retval
