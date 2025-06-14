#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

cd backend && ./manage.py log_broadcaster
