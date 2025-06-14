import json
import os
import sys
import django

sys.path.append('/workspaces/alaz_backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()
from core.serializers import ClusterSerializer

clusters_file = open('/workspace/alaz_backend/scripts/onetime/Cluster.txt', 'r')  # Prod - staging
# clusters_file = open('/workspaces/alaz_backend/backend/scripts/onetime/Cluster.txt', 'r')  # Localhost
clusters = json.loads(clusters_file.read())
for cluster in clusters:
    # It should be in the form
    # {'uid': '', 'name': '', 'user': '', 'monitoring_id': '', 'team': ''}
    serializer = ClusterSerializer(data=cluster)
    if serializer.is_valid():
        try:
            serializer.save()
            print(f'Cluster {cluster["name"]} is recovered')
        except Exception as e:
            print(f'Error while saving cluster: {e}')
    else:
        print(serializer.errors)
clusters_file.close()
print("Clusters are added")