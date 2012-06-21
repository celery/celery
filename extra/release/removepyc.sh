#!/bin/bash
(cd "${1:-.}";
        find . -name "*.pyc" | xargs rm -- 2>/dev/null) || echo "ok"
