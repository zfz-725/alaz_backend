import os
import sys
import django
sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz_backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()
from core.models import Cluster

for cluster in Cluster.objects.all():
    instances = cluster.instances
    for name, ts in instances.items():
        # Check if ts is in seconds
        if ts < 1000000000000:
            instances[name] = ts * 1000
    
    cluster.save()