#!/bin/bash
find "${1:-.}" -name "*.pyc" | xargs rm
find "${1:-.}" -name "*\$py.class" | xargs rm
