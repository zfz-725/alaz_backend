#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

LOG_LISTENER_PORT=9999

cd backend && ./manage.py log_listener $LOG_LISTENER_PORT