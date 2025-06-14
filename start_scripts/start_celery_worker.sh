#!/bin/bash

set -o errexit
set -o nounset

cd backend && celery --app backend worker --concurrency 16 --purge --hostname backendworker1@%h
