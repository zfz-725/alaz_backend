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

batch_size = 100
# Function to process in batches
def process_in_batches(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

# Bulk create them by batch size
clusters_file = open('/workspace/backend/scripts/onetime/AlazCluster.txt', 'r')
clusters = json.loads(clusters_file.read())
for cluster in clusters:
    serializer = ClusterDirectSerializer(data=cluster)
    if serializer.is_valid():
        serializer.save()
    else:
        print(serializer.errors)
clusters_file.close()
print("Clusters are added")