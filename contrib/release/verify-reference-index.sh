#!/bin/bash

verify_index() {
    modules=$(grep "celery." "$1" | \
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

    return $retval
}

verify_index docs/reference/index.rst && \
    verify_index docs/internals/reference/index.rst

