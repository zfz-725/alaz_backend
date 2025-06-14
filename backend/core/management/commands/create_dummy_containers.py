import uuid
from django.core.management.base import BaseCommand
from core.models import Container

class Command(BaseCommand):
    help = 'Populate AlazRequests with fake data'

    def add_arguments(self, parser):
        parser.add_argument('name', type=str, help='Name and namespace of the containers')
        parser.add_argument('pod', type=str, help='Pod uid of the containers')
        parser.add_argument('cluster', type=str, help='Uid of the cluster to add requests to')
        parser.add_argument('count', type=int, help='Number of containers to create')

    def handle(self, *args, **kwargs):
        cluster_uid = kwargs['cluster']
        count = kwargs['count']
        name = kwargs['name']
        pod_uid = kwargs['pod']

        containers = []
        for _ in range(count):
            container = Container(uid=uuid.uuid4(), name=name, namespace=name, cluster=cluster_uid, pod=pod_uid, image=name, ports=[{'src': 80, 'dst': 80}])
            containers.append(container)
        
        Container.objects.bulk_create(containers)
        print(f'Created {count} containers in the pod {pod_uid}')