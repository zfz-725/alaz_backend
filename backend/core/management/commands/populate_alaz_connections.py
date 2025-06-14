from django.core.management.base import BaseCommand
from core.models import Cluster, Connection, Pod, ReplicaSet, Request, Service
from faker import Faker
import random
from datetime import datetime, UTC

fake = Faker()


class Command(BaseCommand):
    help = 'Populate connections with fake data'

    def add_arguments(self, parser):
        # parser.add_argument('count', type=int, help='Number of fake requests to create')
        parser.add_argument('edges', type=int, help='Number of fake edges to create')
        parser.add_argument('req_per_edge', type=int, help='Number of fake connections per edge to create')
        parser.add_argument('cluster', type=str, help='Monitoring id of the cluster to add connections to')

    def handle(self, *args, **kwargs):
        # count = kwargs['count']
        monitoring_id = kwargs['cluster']
        cluster = Cluster.objects.get(monitoring_id=monitoring_id)
        pods = Pod.objects.filter(cluster=cluster.uid)
        replicasets = ReplicaSet.objects.filter(cluster=cluster.uid)
        services = Service.objects.filter(cluster=cluster.uid)
        edge_count = kwargs['edges']
        req_per_edge = kwargs['req_per_edge']
        status_codes = [100, 200, 300, 400, 500]
        endpoints = [fake.uri_path() for _ in range(10)]

        replicasets_cache = {}
        for replicaset in replicasets:
            replicasets_cache[str(replicaset.uid)] = replicaset
            
        pods_cache = {}
        pod_deployments_cache = {}
        for pod in pods:
            pods_cache[str(pod.uid)] = pod
            
            pod_deployments_cache[str(pod.uid)] = None
            
            if pod.replicaset_owner:
                replicaset = replicasets_cache.get(str(pod.replicaset_owner))
                if replicaset:
                    pod_deployments_cache[str(pod.uid)] = replicaset.owner

        bulk_save = []
        size = 0
        flushed = 0

        for _ in range(edge_count):
            type = random.randint(1,6)
            from_uid_pod = None
            from_uid_service = None
            to_uid_pod = None
            to_uid_service = None
            to_url_outbound = None
            if type == 1:
                from_uid_pod = random.choice(pods).uid
                to_uid_pod = random.choice(pods).uid
            elif type == 2:
                from_uid_service = random.choice(services).uid
                to_uid_pod = random.choice(pods).uid
            elif type == 3:
                from_uid_pod = random.choice(pods).uid
                to_uid_service = random.choice(services).uid
            elif type == 4:
                from_uid_service = random.choice(services).uid
                to_uid_service = random.choice(services).uid
            elif type == 5:
                from_uid_pod = random.choice(pods).uid
                to_url_outbound = fake.url()
            elif type == 6:
                from_uid_service = random.choice(services).uid
                to_url_outbound = fake.url()

            if to_uid_service is not None:
                service = Service.objects.get(uid=to_uid_service)
                service_ports = service.ports
                # print(service_ports)
                to_port = random.choice(service_ports)['src']
            else:
                to_port = fake.random_int(min=1, max=65535)

            for _ in range(req_per_edge):
                status_code = random.choice(status_codes)
                connection = Connection(
                    cluster=cluster.uid,
                    timestamp=datetime.now(UTC),
                    from_ip=fake.ipv4(),
                    to_ip=fake.ipv4(),
                    from_port=fake.random_int(min=1, max=65535),
                    to_port=to_port,
                    from_uid_pod=from_uid_pod,
                    from_uid_service=from_uid_service,
                    to_uid_pod=to_uid_pod,
                    to_uid_service=to_uid_service,
                    to_url_outbound=to_url_outbound,
                )
                
                if from_uid_pod:
                    connection.from_uid_deployment = pod_deployments_cache[str(from_uid_pod)]
                    connection.from_uid_daemonset = pods_cache[str(from_uid_pod)].daemonset_owner
                    connection.from_uid_statefulset = pods_cache[str(from_uid_pod)].statefulset_owner
                if to_uid_pod:
                    connection.to_uid_deployment = pod_deployments_cache[str(to_uid_pod)]
                    connection.to_uid_daemonset = pods_cache[str(to_uid_pod)].daemonset_owner
                    connection.to_uid_statefulset = pods_cache[str(to_uid_pod)].statefulset_owner
                    
                bulk_save.append(connection)
                size += 1
                if size == 10000:
                    Connection.objects.bulk_create(bulk_save)
                    bulk_save = []
                    size = 0
                    flushed += 10000
                    print(f'Flushed {flushed} connections in total')
                
        if size > 0:
            Connection.objects.bulk_create(bulk_save)

        self.stdout.write(self.style.SUCCESS(f'Successfully populated {req_per_edge * edge_count} connections.'))