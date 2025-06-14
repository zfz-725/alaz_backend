#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

APP_PORT=8008

# cd backend && ./manage.py migrate && python scripts/generate_users.py && python scripts/generate_periodic_tasks.py && python scripts/generate_settings.py && python manage.py collectstatic --noinput --verbosity 0 && gunicorn --worker-tmp-dir /dev/shm --workers 12 --timeout 90 --bind 0.0.0.0:$APP_PORT backend.wsgi

cd backend && ./manage.py migrate && python scripts/generate_periodic_tasks.py && python scripts/generate_settings.py && python manage.py collectstatic --noinput --verbosity 0 && gunicorn --worker-tmp-dir /dev/shm --workers 12 --timeout 90 --bind 0.0.0.0:$APP_PORT backend.wsgi
