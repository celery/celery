#!/bin/sh

# If you make changes to the celerybeat init script,
# you can use this test script to verify you didn't break the universe

./test_service.sh celerybeat
