#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

APP_PORT=8008

cd backend && ./manage.py migrate && python scripts/generate_settings.py && python manage.py collectstatic --noinput --verbosity 0 && gunicorn --worker-tmp-dir /dev/shm --workers 16 --timeout 120 --bind 0.0.0.0:$APP_PORT -k uvicorn.workers.UvicornWorker backend.asgi:application 

# cd backend && ./manage.py migrate && python scripts/generate_settings.py && python manage.py collectstatic --noinput --verbosity 0 && gunicorn --worker-tmp-dir /dev/shm --workers 8 --timeout 120 --bind 0.0.0.0:$APP_PORT backend.wsgi