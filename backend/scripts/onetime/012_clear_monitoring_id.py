import os
import sys
import django
sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz_backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()
from core.models import Cluster, DaemonSet, Deployment, Endpoint, Pod, ReplicaSet, Service

# Get uid from input

# pod_uid = '1899f451-e66b-45f2-88c2-22519ffe70d6'
pod_uid = input('Pod UID: ')
pod = Pod.objects.get(uid=pod_uid)
cluster = Cluster.objects.get(uid=pod.cluster)

Pod.objects.filter(cluster=cluster.uid).delete()
Deployment.objects.filter(cluster=cluster.uid).delete()
Service.objects.filter(cluster=cluster.uid).delete()
ReplicaSet.objects.filter(cluster=cluster.uid).delete()
DaemonSet.objects.filter(cluster=cluster.uid).delete()
Endpoint.objects.filter(cluster=cluster.uid).delete()
