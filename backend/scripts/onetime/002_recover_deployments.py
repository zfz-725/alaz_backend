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

# Deployment
deployments_file = open('/workspace/backend/scripts/onetime/AlazDeployment.txt', 'r')
deployments = json.loads(deployments_file.read())
batch_num = 0
batch_count = len(deployments) // batch_size
for batch in process_in_batches(deployments, batch_size):
    batch_num += 1
    batch_to_insert = []
    for deployment in batch:
        serializer = DeploymentDirectSerializer(data=deployment)
        if serializer.is_valid():
            batch_to_insert.append(Deployment(**serializer.validated_data))
        else:
            print(serializer.errors)

    # Attempt bulk insert
    try:
        Deployment.objects.bulk_create(batch_to_insert)
        print(f"Batch {batch_num} of {batch_count} is added")
    except Exception as e:
        print(f"Bulk insert failed: {e}, inserting one by one")
        # If bulk insert fails, insert one by one
        for deployment in batch_to_insert:
            try:
                deployment.save()
            except Exception as exc:
                print(f"Error inserting deployment {deployment}: {exc}")
deployments_file.close()
print("Deployments are added")