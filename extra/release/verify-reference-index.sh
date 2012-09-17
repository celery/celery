#!/bin/bash

RETVAL=0

verify_index() {
    retval=0
    for refdir in $*; do
        verify_modules_in_index "$refdir/index.rst"
        verify_files "$refdir"
    done
    return $RETVAL
}

verify_files() {
    for path in $1/*.rst; do
        rst=${path##*/}
        modname=${rst%*.rst}
        if [ $modname != "index" ]; then
            modpath=$(echo $modname | tr . /)
            pkg="$modpath/__init__.py"
            mod="$modpath.py"
            if [ ! -f "$pkg" ]; then
                if [ ! -f "$mod" ]; then
                    echo "*** NO MODULE $modname for reference '$path'"
                    RETVAL=1
                fi
            fi
        fi
    done
}

verify_modules_in_index() {
    modules=$(grep "celery." "$1" | \
                perl -ple's/^\s*|\s*$//g;s{\.}{/}g;')
    for module in $modules; do
        if [ ! -f "$module.py" ]; then
            if [ ! -f "$module/__init__.py" ]; then
                echo "*** IN INDEX BUT NO MODULE: $module"
                RETVAL=1
            fi
        fi
    done
}

verify_index docs/reference docs/internals/reference
