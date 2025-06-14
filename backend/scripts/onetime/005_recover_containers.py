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

# Container
containers_file = open('/workspace/backend/scripts/onetime/AlazContainer.txt', 'r')
containers = json.loads(containers_file.read())
batch_num = 0
batch_count = len(containers) // batch_size
for batch in process_in_batches(containers, batch_size):
    batch_num += 1
    batch_to_insert = []
    for container in batch:
        serializer = ContainerDirectSerializer(data=container)
        if serializer.is_valid():
            batch_to_insert.append(Container(**serializer.validated_data))
        else:
            print(serializer.errors)

    # Attempt bulk insert
    try:
        Container.objects.bulk_create(batch_to_insert)
        print(f"Batch {batch_num} of {batch_count} is added")
    except Exception as e:
        print(f"Bulk insert failed: {e}, inserting one by one")
        # If bulk insert fails, insert one by one
        for container in batch_to_insert:
            try:
                container.save()
            except Exception as exc:
                print(f"Error inserting container {container}: {exc}")
containers_file.close()
print("Containers are added")