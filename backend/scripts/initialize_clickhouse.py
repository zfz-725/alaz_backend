
import json
import os
import sys

import django


sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz-backend/backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
from django.conf import settings
django.setup()
from core.clickhouse_utils import Initializer

clickhouse_initializer = Initializer()
clickhouse_initializer.create_databases()
clickhouse_initializer.create_tables()
clickhouse_initializer.create_materialized_views()