import os
import sys
import uuid
import django
sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz_backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()
from core.models import Cluster
from core.requester import MyBackendRequester
from core.serializers import ClusterDirectSerializer, ClusterSerializer
from django.db import transaction


backend_requester = MyBackendRequester()
cluster_uids = backend_requester.get_cluster_uids()

# clusters_to_delete = []
# clusters_to_create = []

# clusters = Cluster.objects.all()
# for cluster in clusters:
#     if str(cluster.monitoring_id) in cluster_uids:
#         # with transaction.atomic():
#             # cluster.delete()
#         clusters_to_delete.append(cluster)
#         # print(f'Cluster deleted with monitoring id {cluster.monitoring_id}')
#         cluster_data = ClusterDirectSerializer(cluster).data
#         # cluster_data['uid'] = cluster_uids[str(cluster.monitoring_id)]
#         cluster_data['uid'] = uuid.uuid4()
#         clusters_to_create.append(cluster_data)
#         # serializer = ClusterSerializer(data=cluster_data)
#         # print(f'Updating cluster with data {cluster_data}')
#         # print(f'No problems before validation')
#         # if serializer.is_valid():
#         #     print(f'No problems after validation')
#         #     serializer.save()
#         #     print(f'Cluster updated with monitoring id {cluster_data["monitoring_id"]}')
#         # else:
#         #     print(f'Cluster update failed with errors {serializer.errors} with monitoring id {cluster_data["monitoring_id"]}')

Cluster.objects.using('default').all().delete()

for cluster in cluster_uids:
    serializer = ClusterSerializer(data=cluster)
    if serializer.is_valid():
        serializer.save()
        print(f'Cluster created with monitoring id {cluster["monitoring_id"]}')
    else:
        print(f'Cluster creation failed with errors {serializer.errors} with monitoring id {cluster["monitoring_id"]}')
