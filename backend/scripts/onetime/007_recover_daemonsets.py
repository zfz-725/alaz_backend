import json
import os
import sys
import django
sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz_backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()
from core.models import Container, DaemonSet, Deployment, Endpoint, Pod, ReplicaSet, Service

from core.serializers import ClusterDirectSerializer, ContainerDirectSerializer, DaemonSetDirectSerializer, DeploymentDirectSerializer, EndpointDirectSerializer, PodDirectSerializer, ReplicaSetDirectSerializer, ServiceDirectSerializer

# Read them from their files and write them to the db

batch_size = 500
# Function to process in batches
def process_in_batches(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

# DaemonSet
daemonsets_file = open('/workspace/backend/scripts/onetime/AlazDaemonSet.txt', 'r')
daemonsets = json.loads(daemonsets_file.read())
batch_num = 0
batch_count = len(daemonsets) // batch_size
for batch in process_in_batches(daemonsets, batch_size):
    batch_num += 1
    batch_to_insert = []
    for daemonset in batch:
        serializer = DaemonSetDirectSerializer(data=daemonset)
        if serializer.is_valid():
            batch_to_insert.append(DaemonSet(**serializer.validated_data))
        else:
            print(serializer.errors)

    # Attempt bulk insert
    try:
        DaemonSet.objects.bulk_create(batch_to_insert)
        print(f"Batch {batch_num} of {batch_count} is added")
    except Exception as e:
        print(f"Bulk insert failed: {e}, inserting one by one")
        # If bulk insert fails, insert one by one
        for daemonset in batch_to_insert:
            try:
                daemonset.save()
            except Exception as exc:
                print(f"Error inserting daemonset {daemonset}: {exc}")
daemonsets_file.close()
print("DaemonSets are added")
