#!/bin/bash
source {{ additional variables }}
exec celery --app={{ application_name }}._celery:app worker --loglevel=INFO -n worker.%%h